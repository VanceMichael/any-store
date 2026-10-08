package anystore

import (
	"context"
)

// Restore performs a controlled whole-database restore from a single-file
// backup previously written by Backup. See the DB interface declaration for
// the full contract; this file implements the any-store-layer reset on top of
// btree.DB.Restore:
//
//   - the background durability controller is paused for the swap (no
//     checkpoint can race the detached files) and resumed afterwards,
//     including after a rollback;
//   - every Collection handle handed out before the restore is explicitly
//     invalidated (its later operations return ErrCollectionClosed) and the
//     handle registries are cleared — restored content may define entirely
//     different collections under the same names, so cached heads, FTS and
//     vector RAM state must not survive;
//   - the system namespace and schema epoch are rebuilt from the restored
//     catalog, exactly as a fresh Open would see it;
//   - range-index format settlement runs as it does on Open.
//
// The DB object itself survives: instanceId is unchanged and the interface
// value stays usable. Callers re-open collections via OpenCollection/
// Collection after Restore.
func (db *db) Restore(ctx context.Context, backupPath string) (err error) {
	if db.closed.Load() {
		return ErrDBIsClosed
	}
	if ctx.Value(ctxKeyTx) != nil {
		return ErrRestoreInTransaction
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	// Pause the background flush loop WITHOUT touching sentinel state; the
	// swap below needs no checkpoint running. Resume always, on success and
	// after a rollback — the instance is live on one generation or the other.
	if db.recoveryController != nil {
		db.recoveryController.Pause()
	}
	resumed := false
	resume := func() {
		if resumed {
			return
		}
		resumed = true
		if db.recoveryController != nil {
			db.recoveryController.Resume()
		}
	}
	defer resume()

	if err = db.btreeDB.Restore(ctx, backupPath); err != nil {
		return err
	}

	// The storage swap committed. Background flushes are safe again; bring
	// the controller back before our own post-restore write transactions.
	resume()

	return db.afterRestore(ctx)
}

// afterRestore resets every in-memory structure derived from the pre-restore
// content. It runs with no transaction active and the durability controller
// resumed.
func (db *db) afterRestore(ctx context.Context) error {
	// 1. Explicitly invalidate every handed-out collection handle and empty
	// the registries. close(force) only changes in-memory handle state — no
	// data or namespace is dropped.
	var handles []*collection
	db.mu.Lock()
	for _, c := range db.openedCollections {
		handles = append(handles, c.(*collection))
	}
	db.mu.Unlock()
	for _, c := range handles {
		_ = c.close(true)
	}
	db.mu.Lock()
	db.openedCollections = make(map[string]Collection)
	db.byIdentity = make(map[string]*collection)
	db.sketchDirty = nil
	db.ftsDirty = nil
	db.ownCookie.Store(0)
	db.settling.Store(0)
	db.mu.Unlock()

	// 2. Re-acquire the system namespace from the restored catalog.
	db.systemNS = nil
	if err := db.init(ctx); err != nil {
		return err
	}

	// 3. Start a fresh schema epoch at the restored cookie, so every handle
	// a future open creates is proven from the restored catalog rather than
	// inherited. The advisory sketch epoch advances as well.
	_, cookie := db.btreeDB.LocalCounters()
	cur := db.epoch.Load()
	db.epoch.Store(&schemaEpoch{gen: cur.gen + 1, start: cookie, known: cookie})
	db.sketchEpoch.Add(1)

	// 4. Index-format upgrade, exactly as Open runs after a normal open. The
	// backup dropped the settled record, so the restored catalog gets its own
	// one-time scan. A failure leaves the database usable (retried on next
	// Open) but is surfaced to the caller.
	return db.upgradeIndexFormats(ctx)
}
