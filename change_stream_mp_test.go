//go:build (linux || darwin) && (amd64 || arm64)

package anystore

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// a commit made in ANOTHER process reaches an existing stream via the
// database's cross-process visibility channel (WAL frames + mmap
// WAL-index), without the stream restarting.
func TestChangeStream_CrossProcess(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("cross-process visibility requires a file-backed database")
	}
	if path := os.Getenv("CHANGE_STREAM_CHILD_PATH"); path != "" {
		changeStreamCrossProcessChild(t, path)
		return
	}

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	cmd := exec.Command(os.Args[0],
		"-test.run=^TestChangeStream_CrossProcess$",
		"-test.v",
		"-test.timeout=30s",
	)
	dbPath := filepath.Join(fx.tmpDir, "any-store-test.db")
	cmd.Env = append(os.Environ(), "CHANGE_STREAM_CHILD_PATH="+dbPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	// the stream already exists: the child's commit must arrive on it
	rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e, err := s.Next(rctx)
	require.NoError(t, err)
	require.Equal(t, "child", e.Doc.GetString("id"))
	require.Equal(t, uint64(1), e.ResumeToken.Seq())

	require.NoError(t, cmd.Wait())
}

func changeStreamCrossProcessChild(t *testing.T, path string) {
	db, err := Open(context.Background(), path, nil)
	require.NoError(t, err)
	coll, err := db.Collection(context.Background(), "cs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(context.Background(), csDoc("child", 1)))
	require.NoError(t, db.Close())
}
