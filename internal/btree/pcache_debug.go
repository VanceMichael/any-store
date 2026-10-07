package btree

// WriterCachePins reports the pages of the writer's cache and how many of
// them are pinned. Tests only: a cursor left open past its transaction
// leaves its pages pinned for good.
func (db *DB) WriterCachePins() (pages, pinned int) {
	pc := db.pager.writerCache
	for _, head := range pc.apHash {
		for pg := head; pg != nil; pg = pg.hashNext {
			pages++
			if pg.pinCount > 0 {
				pinned++
			}
		}
	}
	return pages, pinned
}
