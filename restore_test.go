package anystore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func openFileDB(t *testing.T, dir string) DB {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(dir, "any-store-test.db"), nil)
	require.NoError(t, err)
	return db
}

func restoreIDs(t *testing.T, coll Collection, filter any) []string {
	t.Helper()
	iter, err := coll.Find(filter).Iter(context.Background())
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, normalizeDocID(d.Value().Get("id").String()))
	}
	require.NoError(t, iter.Err())
	return ids
}

// normalizeDocID accounts for the slight materialization difference between
// iterator paths (some hand back the JSON-quoted string form).
func normalizeDocID(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// TestRestore_FullStack covers the user-facing contract: range/full-text/
// vector indexes, Stats, handle invalidation, post-restore writes, a second
// backup and a cold reopen — all against the restored content.
func TestRestore_FullStack(t *testing.T) {
	skipIfInMemory(t, "restore swaps on-disk generations")
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "any-store-test.db")
	backupPath := filepath.Join(dir, "backup.db")

	db, err := Open(context.Background(), dbPath, nil)
	require.NoError(t, err)

	coll, err := db.CreateCollection(context.Background(), "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(context.Background(),
		IndexInfo{Name: "cat", Fields: []string{"cat"}},
		IndexInfo{Name: "body", Kind: IndexKindFulltext, Fields: []string{"body"}},
		IndexInfo{Name: "v", Kind: IndexKindVector, Vector: &VectorParams{Field: "v", Dim: 4, Metric: VectorL2}},
	))
	require.NoError(t, coll.Insert(context.Background(),
		anyenc.MustParseJson(`{"id":"a","cat":1,"body":"alpha bravo","v":[1,0,0,0]}`),
		anyenc.MustParseJson(`{"id":"b","cat":1,"body":"bravo charlie","v":[0,1,0,0]}`),
	))

	require.NoError(t, db.Backup(context.Background(), backupPath))

	// Diverge: drop+recreate the same-named collection with different
	// content and add a collection the backup never had.
	require.NoError(t, coll.Drop(context.Background()))
	coll2, err := db.CreateCollection(context.Background(), "docs")
	require.NoError(t, err)
	require.NoError(t, coll2.Insert(context.Background(),
		anyenc.MustParseJson(`{"id":"c","cat":9,"body":"totally different","v":[0,0,1,0]}`),
	))
	extra, err := db.CreateCollection(context.Background(), "extra")
	require.NoError(t, err)
	require.NoError(t, extra.Insert(context.Background(), anyenc.MustParseJson(`{"id":"x"}`)))

	// Restore. The DB handle stays usable; the pre-restore coll handle dies.
	require.NoError(t, db.Restore(context.Background(), backupPath))
	_, herr := coll.Count(context.Background())
	assert.ErrorIs(t, herr, ErrCollectionClosed, "pre-restore collection handle must be invalidated")

	// Catalog is the backup's catalog.
	names, err := db.GetCollectionNames(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"docs"}, names)
	_, err = db.OpenCollection(context.Background(), "extra")
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	restored, err := db.OpenCollection(context.Background(), "docs")
	require.NoError(t, err)
	n, err := restored.Count(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	_, err = restored.FindId(context.Background(), "c")
	assert.ErrorIs(t, err, ErrDocNotFound)

	// Range index works against restored bytes.
	ids := restoreIDs(t, restored, map[string]any{"cat": 1})
	assert.ElementsMatch(t, []string{"a", "b"}, ids)

	// Full-text index works against restored sidecar namespaces.
	ids = restoreIDs(t, restored, map[string]any{"$text": map[string]any{"$search": "alpha"}})
	assert.Equal(t, []string{"a"}, ids)

	// Vector index: nearest to a's embedding is a.
	iter, err := restored.Find(`{"v":{"$knn":{"$query":[1,0,0,0],"$k":1}}}`).Iter(context.Background())
	require.NoError(t, err)
	require.True(t, iter.Next())
	d, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, "a", normalizeDocID(d.Value().Get("id").String()))
	require.NoError(t, iter.Close())

	// Stats reflect the restored catalog (1 collection, 3 indexes).
	stats, err := db.Stats(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, stats.CollectionsCount)
	assert.Equal(t, 3, stats.IndexesCount)

	// Post-restore writes work and become searchable.
	require.NoError(t, restored.Insert(context.Background(),
		anyenc.MustParseJson(`{"id":"d","cat":2,"body":"delta echo","v":[0,0,0,1]}`),
	))
	ids = restoreIDs(t, restored, map[string]any{"$text": map[string]any{"$search": "delta"}})
	assert.Equal(t, []string{"d"}, ids)

	// A second backup taken from the restored database is itself consistent
	// and restorable round-trip via a cold open.
	backup2 := filepath.Join(dir, "backup2.db")
	require.NoError(t, db.Backup(context.Background(), backup2))

	require.NoError(t, db.Close())

	reopened := openFileDB(t, dir)
	defer reopened.Close()
	rc, err := reopened.OpenCollection(context.Background(), "docs")
	require.NoError(t, err)
	n, err = rc.Count(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	ids = restoreIDs(t, rc, map[string]any{"$text": map[string]any{"$search": "delta"}})
	assert.Equal(t, []string{"d"}, ids)
	require.NoError(t, reopened.IntegrityCheck(context.Background()))
}

func TestRestore_RejectsAmbientTx(t *testing.T) {
	skipIfInMemory(t, "file backend only")
	dir := t.TempDir()
	db := openFileDB(t, dir)
	defer db.Close()
	backupPath := filepath.Join(dir, "backup.db")
	require.NoError(t, db.Backup(context.Background(), backupPath))

	wtx, err := db.WriteTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = wtx.Rollback() }()
	require.ErrorIs(t, db.Restore(wtx.Context(), backupPath), ErrRestoreInTransaction)
}

func TestRestore_CancellationKeepsOldGeneration(t *testing.T) {
	skipIfInMemory(t, "file backend only")
	dir := t.TempDir()
	db := openFileDB(t, dir)
	defer db.Close()

	coll, err := db.CreateCollection(context.Background(), "docs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(context.Background(),
		anyenc.MustParseJson(`{"id":"keep"}`),
	))
	backupPath := filepath.Join(dir, "backup.db")
	require.NoError(t, db.Backup(context.Background(), backupPath))
	require.NoError(t, coll.Insert(context.Background(),
		anyenc.MustParseJson(`{"id":"later"}`),
	))

	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, db.Restore(cctx, backupPath), context.Canceled)

	// Old generation intact and usable through the same DB handle.
	n, err := coll.Count(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	require.NoError(t, db.IntegrityCheck(context.Background()))
}

func TestRestore_BadBackupRejected(t *testing.T) {
	skipIfInMemory(t, "file backend only")
	dir := t.TempDir()
	db := openFileDB(t, dir)
	defer db.Close()
	coll, err := db.Collection(context.Background(), "docs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(context.Background(), anyenc.MustParseJson(`{"id":1}`)))

	bad := filepath.Join(dir, "bad.db")
	require.NoError(t, os.WriteFile(bad, []byte("this is not a database file at all"), 0o644))
	require.ErrorIs(t, db.Restore(context.Background(), bad), ErrRestoreFormat)

	n, err := coll.Count(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestRestore_InMemoryUnsupported(t *testing.T) {
	db, err := Open(context.Background(), ":memory:", &Config{InMemory: true})
	require.NoError(t, err)
	defer db.Close()
	require.ErrorIs(t, db.Restore(context.Background(), "unused"), ErrRestoreUnsupported)
}
