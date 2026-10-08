package btree

// Controlled whole-database restore ("online restore").
//
// A backup produced by DB.Backup is a single, self-contained main database
// file (its close checkpoints the WAL to zero and unlinks the SHM). Replacing
// a LIVE database's main file by hand is unsafe: the old -wal frames overlay
// the new main by page number and the old -shm hash indexes them, so recovery
// can stitch two generations together. This file implements the controlled
// entry point instead:
//
//	validate (format/structural/encryption/page-size) → copy to a staging
//	generation → quiesce writers, readers, checkpoints and other processes'
//	file locks → checkpoint(TRUNCATE) so the old generation is one standalone
//	file → atomic rename swap guarded by a durable marker → reopen → reset
//	caches.
//
// On-disk protocol (siblings of the main file):
//
//	<path>.restore.d/
//	  new         validated backup copy (a complete generation, no sidecars)
//	  old         previous generation, moved aside during the swap
//	  marker.json generation/commit marker, replaced atomically
//
// A process crashing at the swap boundary leaves the marker behind; the next
// Open runs settleRestoreGenerations under an exclusive file lock and picks
// exactly one COMPLETE generation: "prepared"/"rollback" adopt the old one,
// "committed" adopts the new one. Sidecars (-wal/-shm) are always removed when
// a generation is adopted: neither generation ever opens with the other's
// WAL/SHM. With no marker directory the default open path is unchanged apart
// from one directory stat.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	// restoreStageSuffix is the well-known directory whose presence means a
	// generation swap may need finishing. It is created (by renaming the
	// temporary staging directory) only once the swap is actually starting,
	// together with the prepared marker — so concurrent openers never mistake
	// validation/copy debris for a pending swap.
	restoreStageSuffix = ".restore.d"
	// restoreTmpPrefix names the temporary staging directory used while the
	// backup is copied and validated. Cold Open never scans it; current files
	// stay untouched during that phase and other processes open normally.
	restoreTmpPrefix  = ".restore.stage-"
	restoreNewName    = "new"
	restoreOldName    = "old"
	restoreMarkerName = "marker.json"

	restoreMarkerVersion = 1

	restoreStatePrepared  = "prepared"
	restoreStateCommitted = "committed"
	restoreStateRollback  = "rollback"

	// restoreLockPoll is the interval between lock/quiesce retries while
	// waiting for other processes to release the database.
	restoreLockPoll = 50 * time.Millisecond

	// restoreCopyChunk bounds one read/write of the backup copy, so context
	// cancellation stays responsive on multi-GB backups.
	restoreCopyChunk = 1 << 20
)

var (
	// ErrRestoreUnsupported is returned for in-memory/VFS databases that
	// have no file generation to swap.
	ErrRestoreUnsupported = errors.New("btree: restore is not supported for this database (in-memory)")

	// ErrRestoreFormat is returned when the backup file fails format or
	// structural validation.
	ErrRestoreFormat = errors.New("btree: restore: invalid backup format")

	// ErrRestorePageSize is returned when the backup's page size does not
	// match the live database's page size.
	ErrRestorePageSize = errors.New("btree: restore: backup page size is incompatible with the live database")

	// ErrRestoreEncryption is returned when encryption/checksum integrity
	// modes differ, or the backup cannot be authenticated with the current
	// key.
	ErrRestoreEncryption = errors.New("btree: restore: backup encryption or integrity mode is incompatible")

	// ErrRestoreBusy is returned when the generation swap cannot acquire
	// exclusive ownership of the database cluster (another process still
	// has it open) within the context's deadline.
	ErrRestoreBusy = errors.New("btree: restore: database is busy (other processes still hold it open)")

	// ErrRestoreInProgress is returned by BeginRead/BeginWrite while a
	// restore generation swap is running.
	ErrRestoreInProgress = errors.New("btree: restore: generation swap in progress")
)

// fileID is the durable identity of a generation file, mirroring
// statIdentity. The zero value means "unknown" (non-unix targets).
type fileID struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
	OK  bool   `json:"ok"`
}

func idOf(path string) fileID {
	fi, err := os.Stat(path)
	if err != nil {
		return fileID{}
	}
	dev, ino, ok := statIdentity(fi)
	return fileID{Dev: dev, Ino: ino, OK: ok}
}

func (id fileID) matches(path string) bool {
	if !id.OK {
		return true // identity unavailable on this platform: presence is the policy
	}
	cur := idOf(path)
	if !cur.OK {
		return true
	}
	return cur.Dev == id.Dev && cur.Ino == id.Ino
}

// restoreMarker is the durable state of one generation swap. State transitions:
//
//	prepared ──(renames done)──> committed ──(cleanup)──> marker removed
//	    │
//	    └──(live failure/rollback)──> rollback (adopts old)
type restoreMarker struct {
	Version  int     `json:"version"`
	State    string  `json:"state"`
	PageSize uint32  `json:"page_size"`
	NewID    fileID  `json:"new_id"`
	OldID    *fileID `json:"old_id,omitempty"`
}

// ---------------------------------------------------------------------------
// Marker and staging helpers
// ---------------------------------------------------------------------------

func restoreStageDir(dbPath string) string { return dbPath + restoreStageSuffix }

func markerPath(stage string) string { return filepath.Join(stage, restoreMarkerName) }

func writeRestoreMarker(stage string, m restoreMarker) error {
	m.Version = restoreMarkerVersion
	b, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return err
	}
	final := markerPath(stage)
	tmp := final + ".tmp"
	f, err := osOpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := osRename(tmp, final); err != nil {
		return err
	}
	return fsyncDir(stage)
}

// readRestoreMarker returns (nil, nil) when no marker is present, including
// the case of a crash while the marker itself was being replaced.
func readRestoreMarker(stage string) (*restoreMarker, error) {
	b, err := os.ReadFile(markerPath(stage))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var m restoreMarker
	if err := json.Unmarshal(b, &m); err != nil {
		// A half-written marker is treated as the safe default (old gen).
		return nil, nil
	}
	if m.Version != restoreMarkerVersion {
		return nil, nil
	}
	return &m, nil
}

// removeRestoreSidecars removes any WAL/SHM sibling files. It MUST run while
// adopting a generation: a selected generation is a single main file, and
// attaching a sidecar from the other generation is the exact corruption this
// protocol exists to prevent. Missing files are ignored.
func removeRestoreSidecars(dbPath string) {
	_ = osRemove(dbPath + "-wal")
	_ = osRemove(dbPath + "-wal-shm")
}

func fsyncPath(path string) {
	f, err := osOpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}

// ---------------------------------------------------------------------------
// Cold-start generation resolution
// ---------------------------------------------------------------------------

// settleRestoreGenerations deterministically finishes a generation swap that a
// crashing process left unfinished. It runs before the header is read or the
// codec is built in Open, so the adopted file alone determines format, salt
// and page size. No marker directory means the default open path (one stat).
func settleRestoreGenerations(dbPath string) error {
	stage := restoreStageDir(dbPath)
	fi, err := os.Stat(stage)
	if err != nil || !fi.IsDir() {
		return nil
	}

	// Serialize resolution against concurrent openers and against a live
	// restore holding the generation files: lock whichever generation file
	// is present, exclusively.
	lockFd, err := acquireGenerationLock(dbPath, stage)
	if err != nil {
		return err
	}
	if lockFd != nil {
		defer func() { _ = lockFd.Close() }()
	}

	m, err := readRestoreMarker(stage)
	if err != nil {
		return err
	}
	if m == nil {
		// Staging directory without a usable marker: never a complete swap.
		// Remove only the directory; the main file and its sidecars are
		// untouched.
		_ = os.RemoveAll(stage)
		_ = fsyncDir(filepath.Dir(dbPath))
		return nil
	}
	if err := enforceGeneration(dbPath, stage, m); err != nil {
		return err
	}
	return nil
}

// acquireGenerationLock opens and exclusively locks the first present file of
// main / old / new. A missing main means the swap is mid-rename; the old (or
// new) file still serializes the settlers.
func acquireGenerationLock(dbPath, stage string) (fileHandle, error) {
	candidates := []string{
		dbPath,
		filepath.Join(stage, restoreOldName),
		filepath.Join(stage, restoreNewName),
	}
	for attempt := 0; attempt < 100; attempt++ {
		for _, cand := range candidates {
			f, err := osOpenFile(cand, os.O_RDWR, 0)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, err
			}
			if lerr := acquireExclusiveDBLock(f); lerr == nil {
				return f, nil
			} else if !errors.Is(lerr, ErrBusy) {
				_ = f.Close()
				return nil, lerr
			}
			_ = f.Close()
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("%w: cannot lock any restore generation", ErrRestoreBusy)
}

// enforceGeneration puts the generation selected by the marker at dbPath.
// Presence alone is sufficient on every platform because the swap renames go
// through a state in which the main path is ABSENT (main→old before new→main):
// "main present + old present" after a prepared swap can only mean new is at
// main. Marker identities are an additional contradiction check where inodes
// are available.
func enforceGeneration(dbPath, stage string, m *restoreMarker) error {
	oldPath := filepath.Join(stage, restoreOldName)
	newPath := filepath.Join(stage, restoreNewName)
	_, mainErr := os.Stat(dbPath)
	_, oldErr := os.Stat(oldPath)
	mainExists := mainErr == nil
	oldExists := oldErr == nil

	adoptOld := m.State == restoreStatePrepared || m.State == restoreStateRollback

	if adoptOld {
		switch {
		case !mainExists && oldExists:
			// Interrupted between the two renames.
			if err := osRename(oldPath, dbPath); err != nil {
				return fmt.Errorf("restore settle: rename old generation back: %w", err)
			}
		case mainExists && oldExists:
			// Both present: main is the new generation (see function
			// comment). Discard it and restore the old one. When identities
			// are available, main must be one of the two recorded
			// generations; a third inode here means the directory was
			// tampered with and we refuse to guess.
			cur := idOf(dbPath)
			if cur.OK {
				matchesNew := m.NewID.OK && cur.Dev == m.NewID.Dev && cur.Ino == m.NewID.Ino
				matchesOld := m.OldID != nil && m.OldID.OK && cur.Dev == m.OldID.Dev && cur.Ino == m.OldID.Ino
				if !matchesNew && !matchesOld {
					return fmt.Errorf("%w: restore marker (%s) contradicts files at swap boundary", ErrCorrupt, m.State)
				}
			}
			if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err := osRename(oldPath, dbPath); err != nil {
				return err
			}
		case mainExists && !oldExists:
			// Prepared marker but the swap never started: main already is
			// the old generation. Nothing to move.
		case !mainExists && !oldExists:
			return fmt.Errorf("%w: restore marker %s but neither generation exists", ErrCorrupt, m.State)
		}
	} else {
		// committed: the swap finished; new generation is at dbPath.
		if !mainExists {
			return fmt.Errorf("%w: committed restore marker but main file is missing", ErrCorrupt)
		}
		if !m.NewID.matches(dbPath) {
			return fmt.Errorf("%w: committed restore marker but main file is not the new generation", ErrCorrupt)
		}
		_ = newPath // removed together with the stage directory
	}

	removeRestoreSidecars(dbPath)
	fsyncPath(dbPath)
	if err := fsyncDir(filepath.Dir(dbPath)); err != nil {
		return err
	}
	if err := os.RemoveAll(stage); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(dbPath))
}

// ---------------------------------------------------------------------------
// Live restore
// ---------------------------------------------------------------------------

// testRestoreHook, when set, is invoked at named phase boundaries. Returning
// an error fails the restore at that boundary. Returning the sentinel
// errRestoreSimulatedCrash skips all rollback/cleanup work and shuts the
// handle down, simulating a process crash at the swap boundary. Production
// builds leave it nil (zero overhead).
var testRestoreHook func(phase string) error

// errRestoreSimulatedCrash is the test-only sentinel recognized by Restore.
var errRestoreSimulatedCrash = errors.New("btree: simulated process crash during restore")

// Restore phase names (also used by fault-injection tests).
const (
	phaseCopy            = "copy"
	phaseValidate        = "validate"
	phaseMarkerPrepared  = "marker_prepared"
	phaseDetach          = "detach"
	phaseRenameOld       = "rename_old"
	phaseRenameNew       = "rename_new"
	phaseMarkerCommitted = "marker_committed"
	phaseReopen          = "reopen"
)

func restoreHook(phase string) error {
	if testRestoreHook == nil {
		return nil
	}
	return testRestoreHook(phase)
}

// Restore replaces the live database content with the validated backup at
// backupPath. The DB handle keeps working; observers (in this process and
// other processes opening the cluster) only ever see the pre-restore or the
// post-restore complete state. See the file-level comment for the generation
// protocol.
func (db *DB) Restore(ctx context.Context, backupPath string) (err error) {
	if db.opts.InMemory || db.pager.inMemory {
		return ErrRestoreUnsupported
	}
	if db.closing.Load() || db.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db.restoreMu.Lock()
	defer db.restoreMu.Unlock()

	p := db.pager
	stage := restoreStageDir(p.path)
	tmpStage := filepath.Join(filepath.Dir(p.path),
		filepath.Base(p.path)+restoreTmpPrefix+randomRestoreToken())

	// A live handle cannot coexist with an in-progress swap of another
	// process (it would hold the shared flock that swap upgrades against),
	// so a leftover well-known directory here is debris of an earlier
	// attempt of this process. Remove before staging.
	if rerr := os.RemoveAll(stage); rerr != nil {
		return rerr
	}
	cleanupStaging := func() {
		_ = os.RemoveAll(tmpStage)
		_ = os.RemoveAll(stage)
	}

	// ---- Phases 1-3: verify, copy, structurally validate. The current
	// database files are not touched and the database stays fully online;
	// other processes opening during this phase never see a swap directory.
	newPath, err := db.stageRestoreBackup(ctx, backupPath, tmpStage)
	if err != nil {
		cleanupStaging()
		return err
	}

	// ---- Phase 4: quiesce the instance.
	if err := ctx.Err(); err != nil {
		cleanupStaging()
		return err
	}
	if !db.swapping.CompareAndSwap(false, true) {
		cleanupStaging()
		return ErrRestoreInProgress
	}

	// writeMu serializes writers; db.mu exclusive waits for every in-process
	// read snapshot to finish — the same quiesce Close performs.
	db.writeMu.Lock()
	db.mu.Lock()
	quiesced := false
	releaseQuiesce := func() {
		if quiesced {
			return
		}
		quiesced = true
		db.mu.Unlock()
		db.writeMu.Unlock()
		db.swapping.Store(false)
	}

	failEarly := func(e error) error {
		releaseQuiesce()
		cleanupStaging()
		return e
	}

	if pagerState(p.state.Load()) != pagerOpen {
		return failEarly(fmt.Errorf("%w: pager not idle", ErrRestoreInProgress))
	}
	if err := ctx.Err(); err != nil {
		return failEarly(ctx.Err())
	}

	// ---- Phase 5: exclusive cluster ownership + fold WAL into main.
	//
	// Another process holds a shared flock on the main file for its whole
	// open lifetime; replacing the main file requires it to be gone. Wait
	// under ctx, then fail busy without having changed a file.
	if !p.inProcess {
		if lerr := waitExclusiveUpgrade(ctx, p.file); lerr != nil {
			return failEarly(lerr)
		}
	}
	if cerr := p.checkpointWithMode(CheckpointTruncate); cerr != nil {
		if !p.inProcess {
			_ = downgradeDBLockToShared(p.file)
		}
		return failEarly(fmt.Errorf("%w: checkpoint before restore: %v", ErrRestoreBusy, cerr))
	}
	// The checkpoint fdatasyncs the backfilled main; sync once more so the
	// old generation is durable before it is moved aside.
	if serr := fdatasync(p.file); serr != nil {
		if !p.inProcess {
			_ = downgradeDBLockToShared(p.file)
		}
		return failEarly(serr)
	}

	// ---- Phase 6: publish the staging directory as the well-known swap
	// directory and durably record "prepared" before anything is renamed.
	// From this point on a crash leaves a state the next Open can resolve.
	if rerr := osRename(tmpStage, stage); rerr != nil {
		if !p.inProcess {
			_ = downgradeDBLockToShared(p.file)
		}
		return failEarly(rerr)
	}
	if serr := fsyncDir(filepath.Dir(p.path)); serr != nil {
		if !p.inProcess {
			_ = downgradeDBLockToShared(p.file)
		}
		return failEarly(serr)
	}
	newPath = filepath.Join(stage, restoreNewName)
	oldID := idOfLive(p.file)
	marker := restoreMarker{
		State:    restoreStatePrepared,
		PageSize: p.pageSize,
		NewID:    idOf(newPath),
		OldID:    &oldID,
	}
	if err := writeRestoreMarker(stage, marker); err != nil {
		if !p.inProcess {
			_ = downgradeDBLockToShared(p.file)
		}
		return failEarly(err)
	}
	if herr := restoreHook(phaseMarkerPrepared); herr != nil {
		if errors.Is(herr, errRestoreSimulatedCrash) {
			// Pre-rename crash: pager is still fully attached to the old
			// main file; drop its fd/lock without rollback or cleanup.
			fCrash := p.file
			p.file = nil
			db.crashShutdown(fCrash, nil)
			releaseQuiesce()
			return herr
		}
		if !p.inProcess {
			_ = downgradeDBLockToShared(p.file)
		}
		return failEarly(herr)
	}

	// ---- Phase 7: detach the old generation (main fd stays open & locked).
	//
	// A persistent EX lock on the staged new generation is taken BEFORE the
	// renames so no opener can attach the new main until our reopen finishes.
	fNew, err := osOpenFile(newPath, os.O_RDWR, 0)
	if err != nil {
		return failEarly(err)
	}
	if lerr := acquireExclusiveDBLock(fNew); lerr != nil {
		_ = fNew.Close()
		if !p.inProcess {
			_ = downgradeDBLockToShared(p.file)
		}
		return failEarly(lerr)
	}

	fOld := p.file
	oldPath := filepath.Join(stage, restoreOldName)

	// detachForRestore is best-effort (like p.close's teardown): files are
	// about to be replaced anyway, and a stale unmap/close error must not
	// leave the swap half-announced.
	p.detachForRestore()
	p.file = nil
	if herr := restoreHook(phaseDetach); herr != nil {
		return db.failRestore(herr, stage, fOld, fNew, releaseQuiesce)
	}

	// ---- Phase 8: the atomic swap.
	if rerr := osRename(p.path, oldPath); rerr != nil {
		return db.failRestore(rerr, stage, fOld, fNew, releaseQuiesce)
	}
	if serr := fsyncDir(filepath.Dir(p.path)); serr != nil {
		return db.failRestore(serr, stage, fOld, fNew, releaseQuiesce)
	}
	if herr := restoreHook(phaseRenameOld); herr != nil {
		return db.failRestore(herr, stage, fOld, fNew, releaseQuiesce)
	}
	if rerr := osRename(newPath, p.path); rerr != nil {
		return db.failRestore(rerr, stage, fOld, fNew, releaseQuiesce)
	}
	if serr := fsyncDir(filepath.Dir(p.path)); serr != nil {
		return db.failRestore(serr, stage, fOld, fNew, releaseQuiesce)
	}
	if herr := restoreHook(phaseRenameNew); herr != nil {
		return db.failRestore(herr, stage, fOld, fNew, releaseQuiesce)
	}
	marker.State = restoreStateCommitted
	if werr := writeRestoreMarker(stage, marker); werr != nil {
		return db.failRestore(werr, stage, fOld, fNew, releaseQuiesce)
	}
	if herr := restoreHook(phaseMarkerCommitted); herr != nil {
		return db.failRestore(herr, stage, fOld, fNew, releaseQuiesce)
	}

	// ---- Phase 9: reopen on the new generation.
	// Multi-process: downgrade the new-generation guard to shared so the
	// fresh pager open can take its shared lock; in-process has no peers and
	// closes the guard before its own exclusive open.
	if !p.inProcess {
		if derr := downgradeDBLockToShared(fNew); derr != nil {
			return db.failRestore(derr, stage, fOld, fNew, releaseQuiesce)
		}
	} else {
		_ = fNew.Close()
		fNew = nil
	}
	_ = fOld.Close()
	fOld = nil

	if herr := restoreHook(phaseReopen); herr != nil {
		return db.failRestore(herr, stage, nil, fNew, releaseQuiesce)
	}
	if oerr := p.open(); oerr != nil {
		return db.failRestore(oerr, stage, nil, fNew, releaseQuiesce)
	}
	if fNew != nil {
		_ = fNew.Close()
		fNew = nil
	}
	if rerr := db.afterRestoreReopen(); rerr != nil {
		return db.failRestore(rerr, stage, nil, nil, releaseQuiesce)
	}

	// ---- Success: retire the old generation and the marker. The reopened
	// pager already owns its own fresh -wal/-shm; those must stay.
	if rerr := os.RemoveAll(stage); rerr != nil {
		// Files are swapped and open; leftover debris is cleaned on the next
		// Open. Report but do not pretend the restore failed.
		db.swapping.Store(false)
		db.mu.Unlock()
		db.writeMu.Unlock()
		return nil
	}
	_ = fsyncDir(filepath.Dir(p.path))
	db.swapping.Store(false)
	db.mu.Unlock()
	db.writeMu.Unlock()
	return nil
}

// waitExclusiveUpgrade upgrades the live main file's shared flock to exclusive,
// polling until it succeeds or ctx is done.
func waitExclusiveUpgrade(ctx context.Context, f fileHandle) error {
	for {
		ok, err := tryUpgradeDBLockExclusive(f)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %v", ErrRestoreBusy, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrRestoreBusy, ctx.Err())
		case <-time.After(restoreLockPoll):
		}
	}
}

// idOfLive returns the identity captured from an open fd (works even when the
// path was just renamed).
func idOfLive(f fileHandle) fileID {
	if f == nil {
		return fileID{}
	}
	fi, err := f.Stat()
	if err != nil {
		return fileID{}
	}
	dev, ino, ok := statIdentity(fi)
	return fileID{Dev: dev, Ino: ino, OK: ok}
}

// failRestore handles every failure after detach: a simulated crash leaves
// everything exactly where it is for the next Open to settle; any other
// failure rolls the files back to the old generation and reopens it, keeping
// the live instance working and retryable.
func (db *DB) failRestore(origErr error, stage string, fOld, fNew fileHandle, releaseQuiesce func()) error {
	if errors.Is(origErr, errRestoreSimulatedCrash) {
		db.crashShutdown(fOld, fNew)
		releaseQuiesce()
		return origErr
	}

	p := db.pager

	// Record the rollback decision durably before moving files, so a real
	// crash during this cleanup still resolves to the old generation.
	if m, _ := readRestoreMarker(stage); m != nil {
		m.State = restoreStateRollback
		_ = writeRestoreMarker(stage, *m)
	}

	// Files may currently be: pre-rename (main=old), mid-rename (main
	// missing, old present), or post-rename (main=new, old present). Put the
	// old generation back at p.path. Guards held:
	//   fOld (when non-nil) — EX on the old inode
	//   fNew (when non-nil) — EX/SH on the new inode
	oldPath := filepath.Join(stage, restoreOldName)
	if _, oerr := os.Stat(oldPath); oerr == nil {
		// main present alongside old means main is the new generation; a
		// missing main means the first rename already moved it away.
		_ = os.Remove(p.path)
		if rerr := osRename(oldPath, p.path); rerr != nil && !os.IsNotExist(rerr) {
			releaseQuiesce()
			db.closing.Store(true)
			db.closeOnce.Do(func() { close(db.closeCh) })
			if fNew != nil {
				_ = fNew.Close()
			}
			if fOld != nil {
				_ = fOld.Close()
			}
			return fmt.Errorf("btree: restore rollback failed: %w (restore error: %v)", rerr, origErr)
		}
	}
	removeRestoreSidecars(p.path)
	_ = fsyncDir(filepath.Dir(p.path))

	if fNew != nil {
		_ = fNew.Close()
	}
	// Multi-process: the pager must be able to take its shared lock on the
	// old inode, so downgrade its guard first; in-process closes the guard.
	if fOld != nil {
		if !p.inProcess {
			_ = downgradeDBLockToShared(fOld)
		}
		_ = fOld.Close()
	}

	reopenErr := p.open()
	releaseQuiesce()
	if reopenErr != nil {
		// Even the old generation cannot be reopened: leave the marker in
		// place for the next process and mark the handle dead.
		db.closing.Store(true)
		db.closeOnce.Do(func() { close(db.closeCh) })
		return fmt.Errorf("btree: restore failed and rollback reopen failed: %w (restore error: %v)", reopenErr, origErr)
	}
	if rerr := db.afterRestoreReopen(); rerr != nil {
		db.closing.Store(true)
		db.closeOnce.Do(func() { close(db.closeCh) })
		return fmt.Errorf("btree: restore rolled back but state reset failed: %w (restore error: %v)", rerr, origErr)
	}
	_ = os.RemoveAll(stage)
	_ = fsyncDir(filepath.Dir(p.path))
	return fmt.Errorf("btree: restore failed, rolled back to previous generation: %w", origErr)
}

// crashShutdown mimics an immediate process exit from inside the swap: fds and
// locks are dropped without checkpoint, rollback or cleanup. The marker on
// disk drives the next Open's decision.
func (db *DB) crashShutdown(fOld, fNew fileHandle) {
	p := db.pager
	db.closing.Store(true)
	db.closeOnce.Do(func() { close(db.closeCh) })
	if p.wal != nil {
		_ = p.wal.close(false)
	}
	if fOld != nil {
		_ = fOld.Close()
	}
	if fNew != nil {
		_ = fNew.Close()
	}
}

// afterRestoreReopen rebuilds the DB-level in-memory state the Open tail would
// have produced for the new pager: publish hook, counters, caches.
func (db *DB) afterRestoreReopen() error {
	p := db.pager
	db.installOnPublishHook()

	maxFrame, slot, err := p.beginRead()
	if err != nil {
		return err
	}
	fcc, sc, err := p.readHeaderCounters(maxFrame)
	p.endRead(slot)
	if err != nil {
		return err
	}
	db.localFileChangeCounter.Store(fcc)
	db.localSchemaCookie.Store(sc)
	db.snapCounters = snapCountersMemo{}
	db.commitHook = nil
	db.hasPublished = false
	db.published = WalIndexHdr{}
	db.writerLocksDone.Store(false)
	// Snapshot caches from the old generation must never serve the new one.
	for {
		select {
		case c := <-db.readerCaches:
			c.destroy()
		default:
			goto drained
		}
	}
drained:
	// A monotonically fresh dataVersion additionally invalidates any cache
	// keyed on it; the pooled caches above were already destroyed.
	db.dataVersion.Add(1)

	// Rebuild the codec against the restored page-1 salt/mode. The backup
	// validation proved this exact Options+salt authenticates every page;
	// opening with a stale salt-derived key would otherwise fail AEAD.
	if db.opts.Key != nil || db.opts.Codec != nil {
		var salt [SaltLen]byte
		copy(salt[:], p.header.Salt[:])
		codec, cerr := buildCodec(db.opts, salt[:], false)
		if cerr != nil {
			return cerr
		}
		if sink, ok := codec.(OnErrorSink); ok && db.opts.OnIntegrityError != nil {
			sink.SetOnError(db.opts.OnIntegrityError)
		}
		p.installCodec(codec)
		if p.wal != nil {
			p.wal.codec = codec
		}
	} else if db.opts.Checksum {
		raw := newCksumCodec()
		var codec Codec = raw
		if sink, ok := codec.(OnErrorSink); ok && db.opts.OnIntegrityError != nil {
			sink.SetOnError(db.opts.OnIntegrityError)
		}
		p.installCodec(codec)
		if p.wal != nil {
			p.wal.codec = codec
		}
	}

	// Re-key the process-global open registry on the new main inode.
	if !db.opts.InMemory {
		newKey := registryKeyForOpenFile(p.file, db.path)
		if newKey != db.registryKey {
			openDBs.Delete(db.registryKey)
			openDBs.Store(newKey, true)
			db.registryKey = newKey
		}
	}
	return nil
}

// installOnPublishHook registers the WAL publish callback kept on DB since
// Open. A reopened pager has a fresh wal without it.
func (db *DB) installOnPublishHook() {
	db.pager.wal.onPublish = func(hdr WalIndexHdr) {
		db.snapCounters.put(db.dataVersion.Load(), &hdr, db.pager.header.FileChangeCount, db.pager.header.SchemaCookie)
		if hdr.synthesized() {
			db.published, db.hasPublished = hdr, true
		}
		if f := db.commitHook; f != nil {
			f(db.pager.header.FileChangeCount, db.pager.header.SchemaCookie)
		}
	}
}

// detachForRestore tears down everything besides the locked main fd: mmap,
// WAL (unlinking the SHM as last client) and writer caches. Caller guarantees
// exclusive cluster ownership and a completed TRUNCATE checkpoint. Teardown
// errors are best-effort as in p.close — the files are being replaced anyway.
func (p *pager) detachForRestore() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dbMmap != nil {
		_ = p.dbMmap.unmap()
		p.dbMmap = nil
	}
	if p.wal != nil {
		_ = p.wal.close(true)
		p.wal = nil
	}
	p.backupsMu.Lock()
	p.backups = nil
	p.backupsMu.Unlock()
	p.writerCache.destroy()
	p.savepoints = nil
	p.dontWritePages = nil
	p.hasContent = nil
	p.state.Store(int32(pagerOpen))
	removeRestoreSidecars(p.path)
}

// ---------------------------------------------------------------------------
// Backup validation and staging
// ---------------------------------------------------------------------------

func (db *DB) stageRestoreBackup(ctx context.Context, backupPath, stage string) (string, error) {
	p := db.pager

	absBackup, err := filepath.Abs(backupPath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrRestoreFormat, err)
	}
	if absBackup == p.path || absBackup == p.path+"-wal" || absBackup == p.path+"-wal-shm" {
		return "", fmt.Errorf("%w: backup path must not name the live database files", ErrRestoreFormat)
	}

	backupFi, err := os.Stat(backupPath)
	if err != nil {
		return "", fmt.Errorf("%w: stat backup: %v", ErrRestoreFormat, err)
	}
	if !backupFi.Mode().IsRegular() {
		return "", fmt.Errorf("%w: backup is not a regular file", ErrRestoreFormat)
	}
	if p.file != nil {
		if liveFi, serr := p.file.Stat(); serr == nil && os.SameFile(liveFi, backupFi) {
			return "", fmt.Errorf("%w: backup is the live database file", ErrRestoreFormat)
		}
	}

	// Format/page-size/encryption pre-check from the backup header — before
	// any byte is copied.
	hdr, herr := readInitialHeader(backupPath)
	if herr != nil {
		return "", fmt.Errorf("%w: %v", ErrRestoreFormat, herr)
	}
	if hdr.PageSize != p.pageSize {
		return "", fmt.Errorf("%w: backup page size %d, database %d", ErrRestorePageSize, hdr.PageSize, p.pageSize)
	}
	var zeroSalt [SaltLen]byte
	backupEncrypted := hdr.Salt != zeroSalt
	liveEncrypted := p.header.Salt != zeroSalt
	if backupEncrypted != liveEncrypted {
		return "", fmt.Errorf("%w: backup encrypted=%v database encrypted=%v", ErrRestoreEncryption, backupEncrypted, liveEncrypted)
	}
	backupCksum := !backupEncrypted && hdr.ReservedSpace == cksumOverhead
	liveCksum := !liveEncrypted && p.codec != nil && p.header.ReservedSpace == cksumOverhead
	if backupCksum != liveCksum {
		return "", fmt.Errorf("%w: checksum mode mismatch (backup=%v database=%v)", ErrRestoreEncryption, backupCksum, liveCksum)
	}
	// Reserved space per page is the integrity overhead (0 plain, 16 cksum,
	// 32 GCM/ChaCha20, 48 XChaCha). Equality is required so the reopened
	// pager geometry matches the restored pages; cipher/key mismatches with
	// the same overhead are then caught by AEAD authentication during the
	// structural validation.
	if hdr.ReservedSpace != p.header.ReservedSpace {
		return "", fmt.Errorf("%w: integrity overhead mismatch (backup=%d database=%d)", ErrRestoreEncryption, hdr.ReservedSpace, p.header.ReservedSpace)
	}
	if backupFi.Size() < int64(p.pageSize) || backupFi.Size()%int64(p.pageSize) != 0 {
		return "", fmt.Errorf("%w: size %d is not a whole number of %d-byte pages", ErrRestoreFormat, backupFi.Size(), p.pageSize)
	}

	if err := os.MkdirAll(stage, 0o755); err != nil {
		return "", err
	}
	newPath := filepath.Join(stage, restoreNewName)
	if err := copyFileCtx(ctx, backupPath, newPath, p.pageSize); err != nil {
		return "", err
	}
	fsyncPath(newPath)
	if err := fsyncDir(stage); err != nil {
		return "", err
	}
	if hookErr := restoreHook(phaseCopy); hookErr != nil {
		return "", hookErr
	}

	// Open the copy with the live options: file-state-authoritative checks
	// reject format/page-size drift, and walking every page authenticates
	// AEAD tags / checksum trailers with the current key.
	opts := db.opts
	opts.InMemory = false
	vdb, oerr := Open(newPath, opts)
	if oerr != nil {
		switch {
		case errors.Is(oerr, ErrPageBufferPoolSizeMismatch):
			return "", fmt.Errorf("%w: %v", ErrRestorePageSize, oerr)
		case backupEncrypted:
			return "", fmt.Errorf("%w: cannot open backup with current key/options: %v", ErrRestoreEncryption, oerr)
		default:
			return "", fmt.Errorf("%w: cannot open backup: %v", ErrRestoreFormat, oerr)
		}
	}
	integrityErr := vdb.IntegrityCheck()
	cerr := vdb.Close()
	if integrityErr != nil {
		// On an encrypted copy every page is AEAD-authenticated, so a key
		// mismatch (or any unauthenticatable page) surfaces here rather than
		// as a plain structural failure.
		if backupEncrypted {
			return "", fmt.Errorf("%w: backup cannot be authenticated with the current key: %v", ErrRestoreEncryption, integrityErr)
		}
		return "", fmt.Errorf("%w: structural integrity check failed: %v", ErrRestoreFormat, integrityErr)
	}
	if cerr != nil {
		return "", fmt.Errorf("%w: closing validation copy: %v", ErrRestoreFormat, cerr)
	}
	// The validation copy must stand alone without sidecars (its close
	// truncated the WAL and unlinked the SHM as the last client).
	removeRestoreSidecars(newPath)
	if fi, serr := os.Stat(newPath); serr != nil {
		return "", fmt.Errorf("%w: %v", ErrRestoreFormat, serr)
	} else if fi.Size()%int64(p.pageSize) != 0 || fi.Size() < int64(p.pageSize) {
		return "", fmt.Errorf("%w: validated copy is not a standalone database", ErrRestoreFormat)
	}
	if hookErr := restoreHook(phaseValidate); hookErr != nil {
		return "", hookErr
	}
	return newPath, nil
}

// copyFileCtx streams src to dst (O_EXCL), checking ctx between chunks and
// verifying the final size is a whole number of pages.
func copyFileCtx(ctx context.Context, srcPath, dstPath string, pageSize uint32) error {
	in, err := osOpenFile(srcPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("%w: open backup: %v", ErrRestoreFormat, err)
	}
	defer in.Close()
	out, err := osOpenFile(dstPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("%w: create staging file: %v", ErrRestoreFormat, err)
	}
	buf := make([]byte, restoreCopyChunk)
	var copied int64
	for {
		if err := ctx.Err(); err != nil {
			_ = out.Close()
			return err
		}
		n, rerr := in.Read(buf)
		if n > 0 {
			wn, werr := out.Write(buf[:n])
			if werr != nil {
				_ = out.Close()
				return werr
			}
			if wn != n {
				_ = out.Close()
				return io.ErrShortWrite
			}
			copied += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = out.Close()
			return fmt.Errorf("%w: read backup: %v", ErrRestoreFormat, rerr)
		}
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if copied < int64(pageSize) || copied%int64(pageSize) != 0 {
		return fmt.Errorf("%w: backup size %d is not a whole number of %d-byte pages", ErrRestoreFormat, copied, pageSize)
	}
	return nil
}

// randomRestoreToken names the temporary staging directory uniquely.
func randomRestoreToken() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
