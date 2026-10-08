package anystore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// csDoc builds a {"id":id,"v":v} document.
func csDoc(id string, v int) *anyenc.Value {
	var a anyenc.Arena
	o := a.NewObject()
	o.Set("id", a.NewString(id))
	o.Set("v", a.NewNumberInt(v))
	return o
}

func nextOK(t *testing.T, s ChangeStream) ChangeEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e, err := s.Next(ctx)
	require.NoError(t, err)
	return e
}

func nextTimeout(t *testing.T, s ChangeStream, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, err := s.Next(ctx)
	return err
}

func drainN(t *testing.T, s ChangeStream, n int) []ChangeEvent {
	t.Helper()
	out := make([]ChangeEvent, 0, n)
	for range n {
		out = append(out, nextOK(t, s))
	}
	return out
}

func expectNoEvent(t *testing.T, s ChangeStream) {
	t.Helper()
	require.ErrorIs(t, nextTimeout(t, s, 120*time.Millisecond), context.DeadlineExceeded)
}

func withTxValue(parent context.Context, tx WriteTx) context.Context {
	return context.WithValue(parent, ctxKeyTx, tx)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// insert/update/delete produce events in deterministic in-tx order with
// monotonic positions.
func TestChangeStream_OrderedMonotonicEvents(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx, csDoc("a", 1), csDoc("b", 2)))
	require.NoError(t, coll.UpdateOne(ctx, csDoc("a", 10)))
	require.NoError(t, coll.DeleteId(ctx, "b"))

	evs := drainN(t, s, 4)
	wantOps := []OperationType{OperationInsert, OperationInsert, OperationUpdate, OperationDelete}
	for i, e := range evs {
		require.Equal(t, wantOps[i], e.Operation, "event %d", i)
		require.Equal(t, uint64(i+1), e.ResumeToken.Seq(), "monotonic position")
	}
	require.Equal(t, "a", evs[0].Doc.GetString("id"))
	require.Equal(t, 10, evs[2].Doc.GetInt("v"))
	require.Nil(t, evs[3].Doc)
	expectNoEvent(t, s)
}

// event and document are visible in the same committed snapshot.
func TestChangeStream_EventVisibleWithDocument(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, csDoc("a", 1)))

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	bt := rtx.btreeReadTx()

	dataNS, err := bt.GetNamespace("cs")
	require.NoError(t, err)
	streamNS, err := bt.GetNamespace(changeStreamNsName(coll.(*collection)))
	require.NoError(t, err)

	idKey := anyenc.AppendAnyValue(nil, "a")
	_, err = bt.Get(dataNS, idKey) // document visible
	require.NoError(t, err)

	cursor := bt.NewCursor(streamNS)
	defer cursor.Close()
	require.NoError(t, cursor.Seek(seqKey(1))) // event visible, same snapshot
	require.True(t, cursor.Valid())
	require.NoError(t, rtx.Commit())
}

// resume after full process restart: replays events after the token,
// never the token's event itself.
func TestChangeStream_ResumeAfterReopen(t *testing.T) {
	// an InMemory database cannot be reopened: its contents live only
	// in the handle, so resume across restart is N/A there.
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("resume across reopen requires a file-backed database")
	}
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx, csDoc("a", 1), csDoc("b", 2), csDoc("c", 3)))
	evs := drainN(t, s, 3)
	token := evs[1].ResumeToken // position of b
	require.NoError(t, s.Close())
	require.NoError(t, fx.Close())

	path := filepath.Join(fx.tmpDir, "any-store-test.db")
	db2, err := Open(ctx, path, nil)
	require.NoError(t, err)
	fx.DB = db2 // cleanup closes the reopened handle

	coll2, err := db2.OpenCollection(ctx, "cs")
	require.NoError(t, err)
	s2, err := coll2.Watch(ctx, WatchOptions{ResumeToken: token})
	require.NoError(t, err)

	e := nextOK(t, s2) // first event AFTER the token: c, seq 3
	require.Equal(t, uint64(3), e.ResumeToken.Seq())
	require.Equal(t, "c", e.Doc.GetString("id"))
	expectNoEvent(t, s2)
}

// rolled-back writes, failed commits and savepoint-rolled-back writes
// produce no events.
func TestChangeStream_NoEventsOnRollback(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	// full rollback
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(withTxValue(ctx, wtx), csDoc("rb", 1)))
	require.NoError(t, wtx.Rollback())

	// failed commit: duplicate id fails the whole tx
	require.Error(t, coll.Insert(ctx, csDoc("dup", 1), csDoc("dup", 2)))

	// savepoint rollback
	wtx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	sp, err := fx.WriteTx(withTxValue(ctx, wtx2))
	require.NoError(t, err)
	require.NoError(t, coll.Insert(withTxValue(ctx, sp), csDoc("sp-rb", 1)))
	require.NoError(t, sp.Rollback())
	// an insert outside the rolled-back scope still commits
	require.NoError(t, coll.Insert(withTxValue(ctx, wtx2), csDoc("kept", 1)))
	require.NoError(t, wtx2.Commit())

	evs := drainN(t, s, 1)
	require.Equal(t, "kept", evs[0].Doc.GetString("id"))
	expectNoEvent(t, s)
}

// released savepoint keeps its events at the outer commit.
func TestChangeStream_SavepointReleaseKeepsEvents(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	sp, err := fx.WriteTx(withTxValue(ctx, wtx))
	require.NoError(t, err)
	require.NoError(t, coll.Insert(withTxValue(ctx, sp), csDoc("a", 1)))
	require.NoError(t, sp.Commit()) // RELEASE
	require.NoError(t, wtx.Commit())

	evs := drainN(t, s, 1)
	require.Equal(t, "a", evs[0].Doc.GetString("id"))
}

// multiple subscribers each keep their own position.
func TestChangeStream_MultipleSubscribersIndependent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s1, err := coll.Watch(ctx)
	require.NoError(t, err)
	s2, err := coll.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx, csDoc("a", 1), csDoc("b", 2), csDoc("c", 3)))

	e1 := drainN(t, s1, 3)
	e2 := drainN(t, s2, 3)
	for i := range 3 {
		require.Equal(t, uint64(i+1), e1[i].ResumeToken.Seq())
		require.Equal(t, uint64(i+1), e2[i].ResumeToken.Seq())
		require.Equal(t, e1[i].Doc.GetString("id"), e2[i].Doc.GetString("id"))
	}
}

// WatchStartNow skips history present at Watch time.
func TestChangeStream_StartNow(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, csDoc("old", 1)))

	s, err := coll.Watch(ctx, WatchOptions{StartFrom: WatchStartNow})
	require.NoError(t, err)
	expectNoEvent(t, s)

	require.NoError(t, coll.Insert(ctx, csDoc("new", 1)))
	e := nextOK(t, s)
	require.Equal(t, "new", e.Doc.GetString("id"))
}

// rename keeps the same stream, identity and tokens.
func TestChangeStream_RenameKeepsStream(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx, csDoc("a", 1)))
	e := nextOK(t, s)
	token := e.ResumeToken

	require.NoError(t, coll.Rename(ctx, "renamed"))
	// the live stream survives the rename and continues
	require.NoError(t, coll.Insert(ctx, csDoc("b", 2)))
	e = nextOK(t, s)
	require.Equal(t, "b", e.Doc.GetString("id"))
	require.Equal(t, uint64(2), e.ResumeToken.Seq())

	// opening by the new name and resuming the old token works
	coll2, err := fx.OpenCollection(ctx, "renamed")
	require.NoError(t, err)
	s2, err := coll2.Watch(ctx, WatchOptions{ResumeToken: token})
	require.NoError(t, err)
	e = nextOK(t, s2)
	require.Equal(t, "b", e.Doc.GetString("id"))

	_, err = fx.OpenCollection(ctx, "cs")
	require.ErrorIs(t, err, ErrCollectionNotFound)
}

// drop ends the old stream; a same-named collection cannot continue it.
func TestChangeStream_DropEndsAndRecreateIsolates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx, csDoc("a", 1), csDoc("b", 2)))
	drainN(t, s, 2)

	require.NoError(t, coll.Drop(ctx))
	_, err = s.Next(ctx)
	require.ErrorIs(t, err, ErrChangeStreamEnded)

	coll2, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	// the old token is rejected against the new identity
	_, err = coll2.Watch(ctx, WatchOptions{ResumeToken: newResumeToken(coll.(*collection).catalogTokenBytes(), 1)})
	require.ErrorIs(t, err, ErrInvalidResumeToken)

	s2, err := coll2.Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, coll2.Insert(ctx, csDoc("fresh", 1)))
	e := nextOK(t, s2)
	require.Equal(t, uint64(1), e.ResumeToken.Seq(), "recreated collection restarts the sequence")
	require.Equal(t, "fresh", e.Doc.GetString("id"))
}

// ctx cancellation while waiting does not advance the position and the
// stream retries from the same token.
func TestChangeStream_CancelWaitKeepsToken(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	// wait with no events: cancelled
	require.ErrorIs(t, nextTimeout(t, s, 80*time.Millisecond), context.DeadlineExceeded)
	// retry: events after the cancellation are delivered from seq 1
	require.NoError(t, coll.Insert(ctx, csDoc("a", 1)))
	e := nextOK(t, s)
	require.Equal(t, uint64(1), e.ResumeToken.Seq())

	// same, mid-stream with a saved token
	token := e.ResumeToken
	require.ErrorIs(t, nextTimeout(t, s, 80*time.Millisecond), context.DeadlineExceeded)
	require.NoError(t, coll.Insert(ctx, csDoc("b", 2)))
	e = nextOK(t, s)
	require.Equal(t, uint64(2), e.ResumeToken.Seq())

	// and resume-from-token works on a fresh stream after the cancels
	s3, err := coll.Watch(ctx, WatchOptions{ResumeToken: token})
	require.NoError(t, err)
	e = nextOK(t, s3)
	require.Equal(t, "b", e.Doc.GetString("id"))
}

// Close ends Next with ErrChangeStreamClosed and is idempotent.
func TestChangeStream_Close(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	_, err = s.Next(ctx)
	require.ErrorIs(t, err, ErrChangeStreamClosed)
}

// bounded batches: polling never skips or duplicates across many events.
func TestChangeStream_BatchPolling(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx, WatchOptions{BatchSize: 2})
	require.NoError(t, err)

	docs := make([]*anyenc.Value, 0, 5)
	for i := range 5 {
		docs = append(docs, csDoc(string(rune('a'+i)), i))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	evs := drainN(t, s, 5)
	for i := range 5 {
		require.Equal(t, uint64(i+1), evs[i].ResumeToken.Seq())
	}
	expectNoEvent(t, s)
}

// purge removes confirmed events, advances the anchor, and keeps later
// events; the purge token stays a valid resume anchor.
func TestChangeStream_Purge(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	s, err := coll.Watch(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx, csDoc("a", 1), csDoc("b", 2), csDoc("c", 3)))
	evs := drainN(t, s, 3)
	t1, t2, t3 := evs[0].ResumeToken, evs[1].ResumeToken, evs[2].ResumeToken

	require.NoError(t, coll.PurgeChangeEvents(ctx, t2))

	// beginning now starts at the first retained event
	sBeg, err := coll.Watch(ctx)
	require.NoError(t, err)
	e := nextOK(t, sBeg)
	require.Equal(t, uint64(3), e.ResumeToken.Seq())

	// resume exactly at the purge token still works (anchor)
	sT2, err := coll.Watch(ctx, WatchOptions{ResumeToken: t2})
	require.NoError(t, err)
	e = nextOK(t, sT2)
	require.Equal(t, uint64(3), e.ResumeToken.Seq())

	// tokens before the anchor fail at the first read (the position is
	// only evaluated against the durable anchor when Next polls)
	sT1, err := coll.Watch(ctx, WatchOptions{ResumeToken: t1})
	require.NoError(t, err)
	_, err = sT1.Next(ctx)
	require.ErrorIs(t, err, ErrChangeHistoryLost)

	// events after the token are all present
	sT3, err := coll.Watch(ctx, WatchOptions{ResumeToken: t3})
	require.NoError(t, err)
	expectNoEvent(t, sT3)

	// cannot purge beyond the stream
	require.ErrorIs(t, coll.PurgeChangeEvents(ctx, newResumeToken(coll.(*collection).catalogTokenBytes(), 99)),
		ErrInvalidResumeToken)
}

// backup copies the change stream and remains independently readable.
func TestChangeStream_BackupIncludesEvents(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, csDoc("a", 1), csDoc("b", 2)))

	backupPath := filepath.Join(fx.tmpDir, "backup.db")
	require.NoError(t, fx.Backup(ctx, backupPath))

	db2, err := Open(ctx, backupPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	coll2, err := db2.OpenCollection(ctx, "cs")
	require.NoError(t, err)
	d, err := coll2.FindId(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "a", d.Value().GetString("id"))

	s2, err := coll2.Watch(ctx)
	require.NoError(t, err)
	evs := drainN(t, s2, 2)
	require.Equal(t, uint64(2), evs[1].ResumeToken.Seq())
}
