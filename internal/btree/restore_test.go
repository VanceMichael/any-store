package btree

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// makeBtreeBackup creates a single-file backup of src at dstPath using the
// online backup API, the same way the any-store Backup entry does.
func makeBtreeBackup(t *testing.T, src *DB, dstPath string) {
	t.Helper()
	opts := src.Options()
	opts.InMemory = false
	dst, err := Open(dstPath, opts)
	require.NoError(t, err)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		serr := b.Step(256)
		if errors.Is(serr, ErrBackupDone) {
			break
		}
		require.NoError(t, serr)
	}
	require.NoError(t, b.Finish())
	require.NoError(t, dst.Close())
}

func restoreTestOpts(inProcess bool) Options {
	return Options{
		PageSize:              4096,
		CacheSize:             200,
		InProcess:             inProcess,
		DisableAutoCheckpoint: true,
	}
}

func kvWrite(t *testing.T, db *DB, nsName string, from, to int, prefix string) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace(nsName)
	if errors.Is(err, ErrNamespaceNotFound) {
		require.NoError(t, db.CreateNamespace(tx, nsName))
		ns, err = tx.GetNamespace(nsName)
	}
	require.NoError(t, err)
	for i := from; i <= to; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, []byte(prefix+"-"+string(rune('A'+i%26)))))
	}
	require.NoError(t, tx.Commit())
}

func kvGet(t *testing.T, db *DB, nsName string, i uint32) ([]byte, bool) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err := rtx.GetNamespace(nsName)
	require.NoError(t, err)
	v, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, i))
	if errors.Is(err, ErrKeyNotFound) {
		return nil, false
	}
	require.NoError(t, err)
	return v, true
}

func runBasicRestore(t *testing.T, inProcess bool) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backupPath := filepath.Join(dir, "backup.db")

	db, err := testOpen(t, path, restoreTestOpts(inProcess))
	require.NoError(t, err)

	kvWrite(t, db, "kvs", 1, 50, "old")
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	makeBtreeBackup(t, db, backupPath)

	// Diverge the live database from the backup.
	kvWrite(t, db, "kvs", 51, 100, "new")

	require.NoError(t, db.Restore(context.Background(), backupPath))

	// Old generation WAL must not be attached: the reopened pager starts at
	// frame zero on a fresh WAL.
	require.Equal(t, uint32(0), db.pager.wal.nFrame.Load())

	// Content equals the backup: 1..50 present, 51..100 absent.
	for i := uint32(1); i <= 50; i++ {
		v, ok := kvGet(t, db, "kvs", i)
		require.True(t, ok, "key %d missing after restore", i)
		require.Equal(t, "old-"+string(rune('A'+i%26)), string(v))
	}
	_, ok := kvGet(t, db, "kvs", 51)
	require.False(t, ok)
	_, ok = kvGet(t, db, "kvs", 100)
	require.False(t, ok)

	// Writes after restore land durably and survive a checkpoint+reopen.
	kvWrite(t, db, "kvs", 200, 210, "post")
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())

	db2, err := testOpen(t, path, restoreTestOpts(inProcess))
	require.NoError(t, err)
	defer db2.Close()
	_, ok = kvGet(t, db2, "kvs", 75)
	require.False(t, ok, "post-restore reopen must not see pre-restore-only data")
	v, ok := kvGet(t, db2, "kvs", 200)
	require.True(t, ok)
	require.Equal(t, "post-"+string(rune('A'+200%26)), string(v))
	v, ok = kvGet(t, db2, "kvs", 1)
	require.True(t, ok)
	require.Equal(t, "old-"+string(rune('A'+1%26)), string(v))

	// No staging debris, no shm file after the last close.
	_, err = os.Stat(path + restoreStageSuffix)
	require.True(t, os.IsNotExist(err))
}

func TestRestore_Basic_MultiProcess(t *testing.T) {
	runBasicRestore(t, false)
}

func TestRestore_Basic_InProcess(t *testing.T) {
	runBasicRestore(t, true)
}

func TestRestore_RejectsInvalidBackups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backupPath := filepath.Join(dir, "backup.db")

	db, err := testOpen(t, path, restoreTestOpts(false))
	require.NoError(t, err)
	kvWrite(t, db, "kvs", 1, 10, "old")
	makeBtreeBackup(t, db, backupPath)
	kvWrite(t, db, "kvs", 11, 20, "new")

	t.Run("garbage file", func(t *testing.T) {
		bad := filepath.Join(dir, "garbage.db")
		require.NoError(t, os.WriteFile(bad, make([]byte, 8192), 0o644))
		require.ErrorIs(t, db.Restore(context.Background(), bad), ErrRestoreFormat)
	})
	t.Run("size not a whole page", func(t *testing.T) {
		bad := filepath.Join(dir, "truncated.db")
		require.NoError(t, os.WriteFile(bad, []byte("partial file contents"), 0o644))
		require.ErrorIs(t, db.Restore(context.Background(), bad), ErrRestoreFormat)
	})
	t.Run("tampered page size", func(t *testing.T) {
		bad := filepath.Join(dir, "badsize.db")
		b, rerr := os.ReadFile(backupPath)
		require.NoError(t, rerr)
		binary.BigEndian.PutUint16(b[16:18], 8192)
		require.NoError(t, os.WriteFile(bad, b, 0o644))
		require.ErrorIs(t, db.Restore(context.Background(), bad), ErrRestorePageSize)
	})
	t.Run("backup is the live file", func(t *testing.T) {
		require.ErrorIs(t, db.Restore(context.Background(), path), ErrRestoreFormat)
	})

	// Live database unchanged after every rejection.
	for i := uint32(11); i <= 20; i++ {
		_, ok := kvGet(t, db, "kvs", i)
		require.True(t, ok, "live data must survive failed restore")
	}
	_, statErr := os.Stat(path + restoreStageSuffix)
	require.True(t, os.IsNotExist(statErr))
	require.NoError(t, db.Close())
}

func TestRestore_ContextCancelledBeforeSwap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backupPath := filepath.Join(dir, "backup.db")

	db, err := testOpen(t, path, restoreTestOpts(false))
	require.NoError(t, err)
	kvWrite(t, db, "kvs", 1, 10, "old")
	makeBtreeBackup(t, db, backupPath)
	kvWrite(t, db, "kvs", 11, 20, "new")

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel before validation opens anything: the copy loop checks ctx
	// first, so this simply aborts with the current files untouched.
	cancel()
	require.ErrorIs(t, db.Restore(ctx, backupPath), context.Canceled)
	_, ok := kvGet(t, db, "kvs", 15)
	require.True(t, ok)
	_, statErr := os.Stat(path + restoreStageSuffix)
	require.True(t, os.IsNotExist(statErr))
	require.NoError(t, db.Close())
}

// TestRestore_FailureRollsBack injects an ordinary (non-crash) failure at
// every post-prepared boundary and verifies the instance keeps serving the
// old generation and the restore is retryable.
func TestRestore_FailureRollsBack(t *testing.T) {
	for _, phase := range []string{phaseDetach, phaseRenameOld, phaseRenameNew, phaseMarkerCommitted, phaseReopen} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.db")
			backupPath := filepath.Join(dir, "backup.db")

			db, err := testOpen(t, path, restoreTestOpts(false))
			require.NoError(t, err)
			kvWrite(t, db, "kvs", 1, 20, "old")
			makeBtreeBackup(t, db, backupPath)
			kvWrite(t, db, "kvs", 21, 40, "new")

			injected := errors.New("injected restore failure")
			prev := testRestoreHook
			testRestoreHook = func(p string) error {
				if p == phase {
					return injected
				}
				return nil
			}
			rerr := db.Restore(context.Background(), backupPath)
			testRestoreHook = prev
			require.Error(t, rerr)
			require.ErrorIs(t, rerr, injected)

			// Old generation is live and writable.
			v, ok := kvGet(t, db, "kvs", 30)
			require.True(t, ok, "phase %s: new-gen data must be readable after rollback", phase)
			require.Equal(t, "new-"+string(rune('A'+30%26)), string(v))
			_, ok = kvGet(t, db, "kvs", 5)
			require.True(t, ok)
			kvWrite(t, db, "kvs", 41, 45, "after")

			// No marker/staging leftovers.
			_, statErr := os.Stat(path + restoreStageSuffix)
			require.True(t, os.IsNotExist(statErr), "phase %s: staging dir remains", phase)

			// Retry with no hook succeeds.
			require.NoError(t, db.Restore(context.Background(), backupPath), "phase %s: retry must succeed", phase)
			_, ok = kvGet(t, db, "kvs", 30)
			require.False(t, ok, "phase %s: retry restored backup content", phase)
			v, ok = kvGet(t, db, "kvs", 5)
			require.True(t, ok)
			require.Equal(t, "old-"+string(rune('A'+5%26)), string(v))

			require.NoError(t, db.Close())
		})
	}
}

// TestRestore_CrashBoundaries simulates a process death at each swap
// boundary and checks the NEXT Open deterministically adopts one complete
// generation.
func TestRestore_CrashBoundaries(t *testing.T) {
	cases := []struct {
		phase   string
		wantOld bool // true: old generation; false: new (backup) generation
	}{
		{phaseMarkerPrepared, true},
		{phaseDetach, true},
		{phaseRenameOld, true},
		{phaseRenameNew, true},
		{phaseMarkerCommitted, false},
		{phaseReopen, false},
	}
	for _, tc := range cases {
		t.Run(tc.phase, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.db")
			backupPath := filepath.Join(dir, "backup.db")

			db, err := testOpen(t, path, restoreTestOpts(false))
			require.NoError(t, err)
			regKey := db.registryKey
			kvWrite(t, db, "kvs", 1, 20, "old")
			makeBtreeBackup(t, db, backupPath)
			kvWrite(t, db, "kvs", 21, 40, "new")

			prev := testRestoreHook
			testRestoreHook = func(p string) error {
				if p == tc.phase {
					return errRestoreSimulatedCrash
				}
				return nil
			}
			rerr := db.Restore(context.Background(), backupPath)
			testRestoreHook = prev
			require.ErrorIs(t, rerr, errRestoreSimulatedCrash)

			// Simulate a fresh process: drop the dead handle's registry
			// entry, then open the same path cold. Marker resolution picks
			// one complete generation.
			openDBs.Delete(regKey)
			db2, err := testOpen(t, path, restoreTestOpts(false))
			require.NoError(t, err)

			if tc.wantOld {
				v, ok := kvGet(t, db2, "kvs", 30)
				require.True(t, ok, "old generation must be adopted at %s", tc.phase)
				require.Equal(t, "new-"+string(rune('A'+30%26)), string(v))
			} else {
				_, ok := kvGet(t, db2, "kvs", 30)
				require.False(t, ok, "new generation must be adopted at %s", tc.phase)
				v, ok := kvGet(t, db2, "kvs", 5)
				require.True(t, ok)
				require.Equal(t, "old-"+string(rune('A'+5%26)), string(v))
			}
			_, statErr := os.Stat(path + restoreStageSuffix)
			require.True(t, os.IsNotExist(statErr))
			require.NoError(t, db2.Close())
		})
	}
}

func TestRestore_TxDuringSwapRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backupPath := filepath.Join(dir, "backup.db")

	db, err := testOpen(t, path, restoreTestOpts(false))
	require.NoError(t, err)
	kvWrite(t, db, "kvs", 1, 5, "old")
	makeBtreeBackup(t, db, backupPath)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	prev := testRestoreHook
	testRestoreHook = func(p string) error {
		if p == phaseMarkerPrepared {
			close(entered)
			<-proceed
		}
		return nil
	}

	resErr := make(chan error, 1)
	go func() {
		resErr <- db.Restore(context.Background(), backupPath)
	}()
	<-entered

	_, rerr := db.BeginRead()
	require.ErrorIs(t, rerr, ErrRestoreInProgress)
	_, werr := db.BeginWrite()
	require.ErrorIs(t, werr, ErrRestoreInProgress)

	close(proceed)
	require.NoError(t, <-resErr)
	testRestoreHook = prev

	// After the swap transactions work again.
	v, ok := kvGet(t, db, "kvs", 1)
	require.True(t, ok)
	require.Equal(t, "old-"+string(rune('A'+1%26)), string(v))
	require.NoError(t, db.Close())
}

// TestRestore_ChecksumMode exercises the plain+checksum codec combination.
func TestRestore_ChecksumMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backupPath := filepath.Join(dir, "backup.db")
	opts := restoreTestOpts(false)
	opts.Checksum = true

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	kvWrite(t, db, "kvs", 1, 30, "old")
	makeBtreeBackup(t, db, backupPath)
	kvWrite(t, db, "kvs", 31, 60, "new")

	require.NoError(t, db.Restore(context.Background(), backupPath))
	_, ok := kvGet(t, db, "kvs", 40)
	require.False(t, ok)
	v, ok := kvGet(t, db, "kvs", 10)
	require.True(t, ok)
	require.Equal(t, "old-"+string(rune('A'+10%26)), string(v))
	require.NoError(t, db.Close())
}

// TestRestore_EncryptedRawKey exercises AEAD with a raw 32-byte key.
func TestRestore_EncryptedRawKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc.db")
	backupPath := filepath.Join(dir, "enc-backup.db")
	opts := restoreTestOpts(false)
	opts.Key = make([]byte, KeyLen)
	for i := range opts.Key {
		opts.Key[i] = byte(i + 1)
	}

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	kvWrite(t, db, "kvs", 1, 30, "secret-old")
	makeBtreeBackup(t, db, backupPath)
	kvWrite(t, db, "kvs", 31, 60, "secret-new")

	require.NoError(t, db.Restore(context.Background(), backupPath))
	_, ok := kvGet(t, db, "kvs", 40)
	require.False(t, ok)
	v, ok := kvGet(t, db, "kvs", 10)
	require.True(t, ok)
	require.Equal(t, "secret-old-"+string(rune('A'+10%26)), string(v))
	require.NoError(t, db.Close())

	// Cold reopen of the restored file with the same key works.
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db2.Close()
	v, ok = kvGet(t, db2, "kvs", 10)
	require.True(t, ok)
	require.Equal(t, "secret-old-"+string(rune('A'+10%26)), string(v))
}

// TestRestore_EncryptionCompatibility exercises the encryption compatibility
// gate: wrong key and plain⇄encrypted mismatches must be rejected before any
// current file changes.
func TestRestore_EncryptionCompatibility(t *testing.T) {
	dir := t.TempDir()

	keyA := make([]byte, KeyLen)
	keyB := make([]byte, KeyLen)
	for i := range keyA {
		keyA[i] = byte(i + 1)
		keyB[i] = byte(i + 2)
	}

	// Backup made by a database encrypted with keyB.
	dbB, err := testOpen(t, filepath.Join(dir, "b.db"), func() Options {
		o := restoreTestOpts(false)
		o.Key = keyB
		return o
	}())
	require.NoError(t, err)
	kvWrite(t, dbB, "kvs", 1, 10, "b")
	backupB := filepath.Join(dir, "b-backup.db")
	makeBtreeBackup(t, dbB, backupB)
	require.NoError(t, dbB.Close())

	// Plain backup.
	dbP, err := testOpen(t, filepath.Join(dir, "p.db"), restoreTestOpts(false))
	require.NoError(t, err)
	kvWrite(t, dbP, "kvs", 1, 10, "plain")
	backupP := filepath.Join(dir, "p-backup.db")
	makeBtreeBackup(t, dbP, backupP)
	require.NoError(t, dbP.Close())

	// Encrypted live database rejects both.
	dbA, err := testOpen(t, filepath.Join(dir, "a.db"), func() Options {
		o := restoreTestOpts(false)
		o.Key = keyA
		return o
	}())
	require.NoError(t, err)
	kvWrite(t, dbA, "kvs", 1, 5, "a")
	require.ErrorIs(t, dbA.Restore(context.Background(), backupB), ErrRestoreEncryption)
	require.ErrorIs(t, dbA.Restore(context.Background(), backupP), ErrRestoreEncryption)
	v, ok := kvGet(t, dbA, "kvs", 1)
	require.True(t, ok)
	require.Equal(t, "a-"+string(rune('A'+1%26)), string(v))
	require.NoError(t, dbA.Close())

	// Plain live database rejects an encrypted backup.
	dbP2, err := testOpen(t, filepath.Join(dir, "p2.db"), restoreTestOpts(false))
	require.NoError(t, err)
	require.ErrorIs(t, dbP2.Restore(context.Background(), backupB), ErrRestoreEncryption)
	require.NoError(t, dbP2.Close())
}

// TestRestore_BusyWithOtherProcess spawns a child that holds the database
// open and verifies Restore fails busy without changing anything.
func TestRestore_BusyWithOtherProcess(t *testing.T) {
	if os.Getenv("TEST_RESTORE_CHILD_PATH") != "" {
		path := os.Getenv("TEST_RESTORE_CHILD_PATH")
		db, err := Open(path, restoreTestOpts(false))
		if err != nil {
			os.Exit(1)
		}
		// Stay open long enough for the parent's lock-upgrade attempt; the
		// restore validation phase races this hold window.
		time.Sleep(20 * time.Second)
		_ = db.Close()
		return
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backupPath := filepath.Join(dir, "backup.db")

	db, err := testOpen(t, path, restoreTestOpts(false))
	require.NoError(t, err)
	kvWrite(t, db, "kvs", 1, 10, "old")
	makeBtreeBackup(t, db, backupPath)

	cmd := exec.Command(os.Args[0], "-test.run=^TestRestore_BusyWithOtherProcess$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "TEST_RESTORE_CHILD_PATH="+path)
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Wait() }()

	// Wait for the child to finish opening (the SHM file appears), then a
	// little longer to be sure its shared flock is held.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, lerr := os.Stat(path + "-wal-shm"); lerr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rerr := db.Restore(ctx, backupPath)
	require.ErrorIs(t, rerr, ErrRestoreBusy)

	// Current data intact and instance usable.
	_, ok := kvGet(t, db, "kvs", 1)
	require.True(t, ok)
	_, statErr := os.Stat(path + restoreStageSuffix)
	require.True(t, os.IsNotExist(statErr))
	require.NoError(t, db.Close())
}
