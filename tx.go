package anystore

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/internal/btree"
)

var txVersion atomic.Uint32

func newTxVersion() uint32 {
	if ver := txVersion.Add(1); ver != 0 {
		return ver
	} else {
		return txVersion.Add(1)
	}
}

// WriteTx represents a read-write transaction.
type WriteTx interface {
	// ReadTx is embedded to provide read-only transaction methods.
	ReadTx
	// Rollback rolls back the transaction.
	// Returns an error if the rollback fails.
	Rollback() error

	// SetModified marks the transaction as having made modifications
	// used internally for sentinel mechanism
	SetModified()

	// schemaLog is the transaction's uncommitted schema changes; see
	// txSchema. Unexported: DDL-internal.
	schemaLog() *txSchema

	// The stack of open savepoints; see commonTx.savepoints.
	// Unexported: savepoint-internal.
	savepointOpened(version uint32)
	savepointOpen(version uint32) bool
	savepointEnded(version uint32)
}

// ReadTx represents a read-only transaction.
type ReadTx interface {
	// Context returns the context associated with the transaction.
	Context() context.Context

	// Commit commits the transaction.
	// Returns an error if the commit fails.
	Commit() error

	// Done returns true if the transaction is completed (committed or rolled back).
	Done() bool

	btreeReadTx() *btree.ReadTx
	btreeWriteTx() *btree.WriteTx
	instanceId() string
	dbRef() *db

	// One call on the transaction at a time; see commonTx.opMu.
	lock()
	unlock()
}

type commonTx struct {
	db      *db
	ctx     context.Context
	readTx  *btree.ReadTx
	writeTx *btree.WriteTx
	version atomic.Uint32
	// modified: SetModified is public, so a caller may set it outside opMu.
	modified atomic.Bool

	// opMu serializes the calls made on the transaction: every operation
	// run with its context, the savepoints opened on it, its commit and
	// rollback. What a transaction holds — the page cache of its btree tx,
	// the full-text buffers, the schema log, the savepoint stack — is built
	// for one caller at a time, so a transaction several goroutines share
	// runs their calls one after another, as SQLite's serialized threading
	// mode runs the calls made on one connection (sqlite3_mutex_enter on
	// db->mutex at every entry point). Taken at the entry points
	// (lockCtxTx, enterWriteTx, the methods of the transaction, of a
	// savepoint, of an iterator), never held when control returns to the
	// caller: an iterator or a savepoint the caller keeps relocks per call.
	// A call made from inside another call — through the context an
	// internal savepoint hands a callback (heldTx) — runs under the
	// enclosing call's turn. A call that waited re-checks Done under the
	// lock: the call it waited for may have ended the transaction, and
	// this state may already be pooled.
	opMu sync.Mutex

	// schema is the transaction's own schema state (txSchema), in the
	// btree transaction's Aux slot for the resolve of every operation. For a
	// write transaction its log holds the uncommitted DDL: the writes later
	// in the same tx see and maintain an index or collection created earlier
	// in it through the log, nothing else does until the commit installs
	// it, and a rollback — full, failed-commit, or to a savepoint
	// (savepointTx records a mark) — discards the scope's entries. Writer
	// state: mutated under the btree write lock.
	schema txSchema

	// savepoints is the stack of open savepoints, outermost first, each by
	// its version. A savepoint that ends takes the savepoints opened inside
	// it with it — SQLite destroys every savepoint nested inside the one a
	// RELEASE or ROLLBACK TO names (vdbe.c, OP_Savepoint). One that is no
	// longer on the stack is gone: its btree savepoint id may name another
	// savepoint by now, and its log mark cuts the log at a position that
	// pairs nothing. Single-writer state, like the log.
	savepoints []uint32
}

func (tx *commonTx) savepointOpened(version uint32) {
	tx.savepoints = append(tx.savepoints, version)
}

func (tx *commonTx) savepointOpen(version uint32) bool {
	return slices.Contains(tx.savepoints, version)
}

// savepointEnded pops the savepoint and every savepoint opened inside it.
func (tx *commonTx) savepointEnded(version uint32) {
	if i := slices.Index(tx.savepoints, version); i >= 0 {
		tx.savepoints = tx.savepoints[:i]
	}
}

func (tx *commonTx) schemaLog() *txSchema {
	return &tx.schema
}

// schemaCommitted runs inside the btree commit as it becomes visible
// (btree.WriteTx.OnCommitted): the logged versions become the heads, the
// registry follows them, then the epoch's known passes the commit's cookie
// — so a transaction the epoch admits finds the handles and the registry
// as the commit left them.
func (tx *commonTx) schemaCommitted(fileChangeCounter, schemaCookie uint32) {
	if testHookBeforeInstall != nil {
		testHookBeforeInstall()
	}
	// settling from before the first handle changes — the install closes
	// the dropped ones, and a slot held by a closed handle is one a
	// registration may take — until the epoch has moved (sinceNow).
	tx.db.settling.Store(1<<32 | uint64(schemaCookie))
	defer tx.db.settling.Store(0)
	tx.schema.install(tx.db.epoch.Load().gen)
	if testHookAfterInstall != nil {
		testHookAfterInstall()
	}
	tx.db.settleLog(&tx.schema)
	if testHookAfterSettle != nil {
		testHookAfterSettle()
	}
	tx.db.schemaCommitted(fileChangeCounter, schemaCookie)
}

// testHookBeforeInstall, when set, runs inside a schema-changing commit as
// it becomes visible, before the heads are installed; testHookAfterInstall
// once they are, before the registry is settled; testHookAfterSettle once
// it is, before the epoch moves; testHookAfterBtreeCommit right after the
// btree commit returned, the write lock released. Tests only.
var (
	testHookBeforeInstall    func()
	testHookAfterInstall     func()
	testHookAfterSettle      func()
	testHookAfterBtreeCommit func()
)

// release returns the pooled state, with nothing pinned. A log still there
// belongs to a commit that did not come back (a panic in the btree): its
// handles are released as after a rollback.
func (tx *commonTx) release() {
	if len(tx.schema.log) > 0 {
		tx.db.discardLog(&tx.schema, 0)
	}
	tx.schema.release()
	txPool.Put(tx)
}

func (tx *commonTx) btreeReadTx() *btree.ReadTx {
	return tx.readTx
}

func (tx *commonTx) btreeWriteTx() *btree.WriteTx {
	return tx.writeTx
}

func (tx *commonTx) SetModified() {
	tx.modified.Store(true)
}

func (tx *commonTx) lock() {
	tx.opMu.Lock()
}

func (tx *commonTx) unlock() {
	tx.opMu.Unlock()
}

// heldTx is the transaction as the context of an internal savepoint carries
// it (savepointTx.Context): the enclosing call holds its lock, so a call
// made through that context runs under its turn instead of locking again —
// the aggregation sink opens or creates its target inside its own write
// scope this way. The embedded WriteTx satisfies every assertion made on
// the context's value.
type heldTx struct{ WriteTx }

func (tx *commonTx) instanceId() string {
	return tx.db.instanceId
}

func (tx *commonTx) dbRef() *db {
	return tx.db
}

var txPool = &sync.Pool{
	New: func() any {
		return &commonTx{}
	},
}

type readTx struct {
	*commonTx
	version uint32
}

func (r readTx) Context() context.Context {
	return r.ctx
}

func (r readTx) Commit() error {
	r.commonTx.lock()
	defer r.commonTx.unlock()
	if r.commonTx.version.CompareAndSwap(r.version, 0) {
		defer r.commonTx.release()
		return r.readTx.Rollback()
	}
	return nil
}

func (r readTx) Done() bool {
	return r.commonTx.version.Load() != r.version
}

type writeTx struct {
	*commonTx
	version uint32
}

func (w writeTx) Context() context.Context {
	return w.ctx
}

// unwind discards this tx's buffered full-text postings and its schema log,
// then rolls the btree tx back. The postings go at rollback, as FTS5
// discards its pending data in xRollback. Both go before the btree releases
// the global write lock inside Rollback: the log is writer state.
func (w writeTx) unwind() error {
	w.db.resetAllFtsPending(w.readTx)
	w.db.discardLog(&w.commonTx.schema, 0)
	return w.writeTx.Rollback()
}

func (w writeTx) Rollback() error {
	w.commonTx.lock()
	defer w.commonTx.unlock()
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		defer w.commonTx.release()
		return w.unwind()
	}
	return nil
}

func (w writeTx) Commit() error {
	w.commonTx.lock()
	defer w.commonTx.unlock()
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		// The btree commit releases the global write lock before it
		// returns. A schema-changing commit announces the cookie it
		// produces (observeCookie), and the announcement of one that
		// publishes nothing — empty, or failed — is withdrawn only once it
		// returned; a log the commit did not publish — a pin-only one, or
		// one of a commit that failed — is discarded then too, and touches
		// the handles' marks. Hold the schema gate (paired with newWriteTx)
		// until then, so no writer of this process begins while another
		// process's commit could pass for this one's, or while the marks
		// move. Taken before the release is deferred: what the release has
		// left to discard, it discards under the gate.
		t := &w.commonTx.schema
		schemaChange := w.writeTx.SchemaChanged()
		if schemaChange || len(t.log) > 0 {
			w.db.schemaGate.Lock()
			defer w.db.schemaGate.Unlock()
		}
		defer w.commonTx.release()
		// Every non-committed exit below must discard the schema log
		// BEFORE its btree rollback releases the global write lock (see
		// unwind).
		if w.modified.Load() {
			w.writeTx.MarkDataChanged()
			// Flush the full-text write-back buffer into this same tx BEFORE the
			// btree commit, so postings commit atomically with the documents.
			if err := w.db.flushAllFtsPending(w.writeTx); err != nil {
				_ = w.unwind()
				return err
			}
			if err := w.db.persistAllDirtySketches(w.writeTx); err != nil {
				_ = w.unwind()
				return err
			}
		}
		if w.writeTx.SchemaChanged() && w.writeTx.HasChanges() {
			// See formatSettledKey. An empty commit (EnsureIndex of an
			// existing index) bumps no cookie and must stay empty.
			if err := w.db.advanceFormatSettled(w.writeTx); err != nil {
				_ = w.unwind()
				return err
			}
		}
		if schemaChange {
			// The logged versions become the heads and the registry follows
			// them as the commit becomes visible, and the epoch follows the
			// cookie right after (commonTx.schemaCommitted). Withdrawn on
			// every exit, a panic in the commit included: an announcement
			// left standing would make the next process's commit at that
			// cookie pass for this one's.
			next := w.writeTx.DiskSchemaCookie() + 1
			w.db.announceCookie(next)
			defer w.db.endAnnouncement(next)
			w.writeTx.OnCommitted(w.commonTx.schemaCommitted)
		}
		err := w.writeTx.Commit()
		if testHookAfterBtreeCommit != nil {
			testHookAfterBtreeCommit()
		}
		if err == nil && w.modified.Load() {
			w.db.recoveryController.OnWriteEvent()
		}
		// A log the commit did not publish is discarded, as after a rollback.
		if !t.installed {
			w.db.discardLog(t, 0)
		}
		return err
	}
	return nil
}

func (w writeTx) Done() bool {
	return w.commonTx.version.Load() != w.version
}

var savepointPool = &sync.Pool{
	New: func() any {
		return &savepointTx{}
	},
}

// newSavepointTx opens a savepoint on wrTx, whose lock the caller holds.
// held marks a savepoint of an enclosing call's scope (enterWriteTx): its
// own calls lock nothing, and the context it hands out marks the turn as
// held for the calls made through it.
func newSavepointTx(ctx context.Context, wrTx WriteTx, held bool) (WriteTx, error) {
	btWtx := wrTx.btreeWriteTx()
	// Flush buffered full-text writes BEFORE creating the savepoint, so their
	// btree writes land outside its scope. This keeps the fts pending buffer
	// savepoint-consistent: at every savepoint creation the buffer is empty, so
	// after RollbackToSavepoint (a) the buffer holds only ops made inside the
	// rolled-back scope (discarded by resetAllFtsPending), and (b) everything
	// flushed after this point sits inside the scope and is reverted by the
	// btree itself. Without this, buffered postings from before the savepoint
	// survive its rollback and flush at outer commit — while the seq/docmap
	// writes they depend on were reverted, attributing ghost postings to
	// whichever document later reuses the IntDocID.
	if err := wrTx.dbRef().flushAllFtsPending(btWtx); err != nil {
		return nil, err
	}
	spId, err := btWtx.Savepoint()
	if err != nil {
		return nil, err
	}
	tx := savepointPool.Get().(*savepointTx)
	tx.reset(wrTx, spId, held)
	version := tx.version.Load()
	wrTx.savepointOpened(version)
	return savepointWrapper{savepointTx: tx, version: version}, nil
}

type savepointWrapper struct {
	*savepointTx
	version uint32
}

type savepointTx struct {
	WriteTx
	savepointId int
	// logMark is the parent tx's schema log length at savepoint creation: a
	// rollback to this savepoint discards exactly the schema changes made
	// inside its scope (entries [logMark:]); a release keeps them on the
	// parent so an outer rollback still discards them.
	logMark int
	version atomic.Uint32
	// held: see newSavepointTx. ctx is the Context of a held savepoint,
	// built on first use.
	held bool
	ctx  context.Context
}

func (tx *savepointTx) reset(wtx WriteTx, spId int, held bool) {
	tx.WriteTx = wtx
	tx.savepointId = spId
	tx.logMark = len(wtx.schemaLog().log)
	tx.held = held
	tx.ctx = nil
	tx.version.Store(newTxVersion())
}

// Context is the transaction's context — carrying the transaction as held
// (heldTx) for a held savepoint, so that the calls a callback makes through
// it run under the enclosing call's turn.
func (tx *savepointTx) Context() context.Context {
	if !tx.held {
		return tx.WriteTx.Context()
	}
	if tx.ctx == nil {
		tx.ctx = context.WithValue(tx.WriteTx.Context(), ctxKeyTx, heldTx{tx.WriteTx})
	}
	return tx.ctx
}

// orphaned reports that the savepoint no longer exists: the transaction it
// belongs to has ended, or a savepoint enclosing it has. SQLite frees every
// savepoint when the transaction ends (sqlite3CloseSavepoints) and the nested
// ones when a savepoint ends (see commonTx.savepoints); a later RELEASE or
// ROLLBACK TO fails with "no such savepoint". The parent's pooled state, the
// btree tx and the writer's buffers may already serve another transaction,
// and the btree savepoint id another savepoint: touch nothing.
func (w savepointWrapper) orphaned() bool {
	return w.WriteTx.Done() || !w.WriteTx.savepointOpen(w.version)
}

func (w savepointWrapper) Commit() error {
	if w.savepointTx.version.CompareAndSwap(w.version, 0) {
		// The parent is read before the pool takes the savepoint back:
		// the unlock must go to the transaction this call locked.
		parent := w.WriteTx
		if !w.held {
			parent.lock()
			defer parent.unlock()
		}
		if w.orphaned() {
			savepointPool.Put(w.savepointTx)
			return ErrTxIsUsed
		}
		w.WriteTx.savepointEnded(w.version)
		btWtx := w.WriteTx.btreeWriteTx()
		if err := btWtx.ReleaseSavepoint(w.savepointId); err != nil {
			return err
		}
		savepointPool.Put(w.savepointTx)
	}
	return nil
}

func (w savepointWrapper) Rollback() error {
	if w.savepointTx.version.CompareAndSwap(w.version, 0) {
		parent := w.WriteTx
		if !w.held {
			parent.lock()
			defer parent.unlock()
		}
		if w.orphaned() {
			savepointPool.Put(w.savepointTx)
			return ErrTxIsUsed
		}
		w.WriteTx.savepointEnded(w.version)
		btWtx := w.WriteTx.btreeWriteTx()
		db := w.WriteTx.dbRef()
		err := btWtx.RollbackToSavepoint(w.savepointId)
		// The fts pending buffers hold only ops made inside this savepoint's
		// scope (they were flushed empty at its creation), and the btree state
		// those ops were derived from has just been reverted — discard them,
		// with the scope's schema log. Both run even when RollbackToSavepoint
		// fails: its error returns happen before any mutation, the outer tx is
		// doomed either way, and matching the in-memory schema state to the
		// last committed disk state is the conservative choice.
		// RollbackToSavepoint keeps the write lock in all cases, so the
		// discard runs inside the critical section.
		db.resetAllFtsPending(&btWtx.ReadTx)
		db.discardLog(w.WriteTx.schemaLog(), w.logMark)
		if err != nil {
			return err
		}
		savepointPool.Put(w.savepointTx)
	}
	return nil
}

func (w savepointWrapper) Done() bool {
	if w.savepointTx.version.Load() != w.version {
		return true
	}
	parent := w.WriteTx
	if !w.held {
		parent.lock()
		defer parent.unlock()
	}
	return w.orphaned()
}

// noOpTx is the transaction a context carries as an operation run with
// that context gets it (getReadTx): the end of the operation — Commit —
// leaves the transaction open and, when the operation locked it, ends the
// operation's turn.
type noOpTx struct {
	ReadTx
	locked bool
}

func (tx noOpTx) Commit() error {
	if tx.locked {
		tx.unlock()
	}
	return nil
}

func (tx noOpTx) Rollback() error {
	return tx.Commit()
}

// detachLock is for a caller that keeps a transaction getReadTx returned
// beyond the call (an iterator): it returns the transaction without the
// turn the call holds — its Commit then leaves the lock alone — and the
// transaction to relock per later call, nil when nobody else can reach it
// (a fresh read transaction, or one an enclosing call holds).
func detachLock(tx ReadTx) (ReadTx, ReadTx) {
	if nt, ok := tx.(noOpTx); ok && nt.locked {
		return noOpTx{ReadTx: nt.ReadTx}, nt.ReadTx
	}
	return tx, nil
}
