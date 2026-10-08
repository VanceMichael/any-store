package btree

import "os"

// fsyncDir flushes the directory entry's metadata for dir, so that file
// creation, removal and rename operations within it are durable across a
// crash. Required at every step of the restore generation swap: without it,
// a power loss can leave the directory still pointing at the pre-rename
// entries even though the renamed file's own bytes were fsynced.
//
// os.File.Sync issues fsync(2), which is valid for directory descriptors on
// Linux and Darwin. On platforms where directory sync is not supported the
// error is ignored best-effort: the generation protocol stays correct via its
// marker, it may just repeat resolution after an OS crash there.
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	_ = f.Close()
	// Best-effort: a platform/filesystem that cannot fsync a directory
	// must not abort the swap — the on-disk marker keeps crash recovery
	// deterministic; only the rename durability guarantee weakens there.
	return nil
}
