package anystore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/syncpool"
)

// defaultTTLSweepInterval bounds the time between TTL sweeps when the next
// known expiration does not wake the monitor earlier.
const defaultTTLSweepInterval = 60 * time.Second

// ttlDeleteBatch caps document deletions per sweep transaction. The cap
// bounds a single transaction's size (and its page-cache footprint); a
// backlog is drained in as many transactions as needed, each starting from
// a fresh writer snapshot — a busy result, context cancellation or any
// other failure therefore rolls back only the unfinished batch, and every
// expired document is found again by the next cycle or reopen.
const ttlDeleteBatch = 1000

// ttlMonitor is the database-scoped TTL cleanup task.
//
// Lifecycle:
//
//   - A database whose schema carries NO TTL index starts no monitor:
//     Open reads the catalog once and returns.
//   - Open on a database WITH a TTL index first runs the opening sweep
//     synchronously (deadlines missed while closed are removed), then the
//     background loop starts.
//   - Creating the first TTL index in this process starts the loop
//     (ensureStarted). The loop discovers its work from the catalog on
//     every cycle and holds NO persistent task state: dropping the last
//     TTL index, dropping the collection, renames and process restarts
//     can therefore neither revive an old task nor delete a document a
//     current definition does not expire.
//   - DB Close cancels the loop and waits for an in-flight sweep to
//     release the write lock.
//
// Every deletion goes through the same write transaction and
// collection.deleteItem path as a user DeleteId, so the data tree and all
// range / full-text / vector derived indexes converge in one commit; a
// sweep transaction never writes schema metadata.
type ttlMonitor struct {
	db       *db
	interval time.Duration
	// nowMilli returns the current time as Unix milliseconds. It is a
	// field (not a package var) so tests can pin the clock per database.
	nowMilli func() int64
	// suspendAuto, when set, makes launch mark the monitor as running
	// without starting the loop goroutine. Tests only: deterministic
	// tests drive sweeps through sweepOnce; nothing in production sets it.
	suspendAuto bool

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func newTTLMonitor(ds *db, cfg TTLConfig) *ttlMonitor {
	interval := cfg.SweepInterval
	if interval <= 0 {
		interval = defaultTTLSweepInterval
	}
	return &ttlMonitor{
		db:       ds,
		interval: interval,
		nowMilli: func() int64 { return time.Now().UnixMilli() },
	}
}

// startup runs at Open. It performs the opening missed-deadline sweep
// synchronously when the catalog carries a TTL index, and starts the
// background loop. A database without a TTL index does nothing here.
// Only a cancelled context fails Open; any other read failure is left for
// the loop (same best-effort policy as upgradeIndexFormats).
func (m *ttlMonitor) startup(ctx context.Context) error {
	cols, err := m.ttlCollections(ctx)
	if err != nil {
		return ctx.Err()
	}
	if len(cols) == 0 {
		return nil
	}
	_, _ = m.sweepOnce(ctx, cols, m.nowMilli())
	if err = ctx.Err(); err != nil {
		return err
	}
	m.launch()
	return nil
}

// ensureStarted starts the background loop after this process created a
// TTL index in a database that previously had none. Idempotent.
func (m *ttlMonitor) ensureStarted() {
	if m == nil {
		return
	}
	m.launch()
}

// stop cancels the loop and waits for it to finish. Safe to call when the
// monitor never started.
func (m *ttlMonitor) stop() error {
	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	return nil
}

func (m *ttlMonitor) launch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running || m.db.closed.Load() {
		return
	}
	if m.suspendAuto {
		// Tests only: keep the "a task exists for this TTL index" state
		// observable, but let the test drive sweeps manually.
		m.running = true
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.running = true
	m.wg.Add(1)
	go m.run(ctx)
}

func (m *ttlMonitor) run(ctx context.Context) {
	defer m.wg.Done()
	for {
		cols, err := m.ttlCollections(ctx)
		if err != nil {
			if ctx.Err() != nil {
				m.markStopped()
				return
			}
			// A catalog read failure costs one wait, then a retry; no
			// expiration record is held in memory, so nothing is lost.
			if !m.sleep(ctx, m.interval) {
				m.markStopped()
				return
			}
			continue
		}
		if len(cols) == 0 {
			// No TTL index in the catalog anymore: the task ends. A TTL
			// index created afterwards in this process calls
			// ensureStarted; a peer-created one is served by the peer
			// and an reopen sweeps it.
			m.markStopped()
			return
		}
		_, earliest := m.sweepOnce(ctx, cols, m.nowMilli())
		if ctx.Err() != nil {
			m.markStopped()
			return
		}
		wait := m.interval
		if earliest < math.MaxInt64 {
			if d := time.Until(time.UnixMilli(earliest)); d > 0 {
				// Wake at the next boundary (never below 1ms), but no
				// later than the configured interval.
				if d < wait {
					wait = d
				}
			} else {
				wait = time.Millisecond
			}
		}
		if !m.sleep(ctx, wait) {
			m.markStopped()
			return
		}
	}
}

func (m *ttlMonitor) markStopped() {
	m.mu.Lock()
	m.running = false
	m.cancel = nil
	m.mu.Unlock()
}

func (m *ttlMonitor) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ttlCollections lists the names of collections that currently carry at
// least one TTL index, read from the catalog in one read transaction.
func (m *ttlMonitor) ttlCollections(ctx context.Context) (cols []string, err error) {
	err = m.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		names, nErr := m.db.collectionNames(tx)
		if nErr != nil {
			return nErr
		}
		for _, name := range names {
			infos, iErr := m.db.getIndexInfos(tx, name)
			if iErr != nil {
				return iErr
			}
			for i := range infos {
				if infos[i].TTL != nil {
					cols = append(cols, name)
					break
				}
			}
		}
		return nil
	})
	return cols, err
}

// sweepOnce deletes expired documents of the TTL indexes on the given
// collections, each collection drained in bounded write transactions. It
// returns the number of deleted documents and the earliest upcoming
// expiration boundary (Unix ms; math.MaxInt64 when none is known). A
// failure on one collection ends the sweep: no progress record is kept,
// and the next cycle or reopen finds the same documents again.
func (m *ttlMonitor) sweepOnce(ctx context.Context, cols []string, nowMs int64) (deleted int, earliest int64) {
	earliest = math.MaxInt64
	for _, coll := range cols {
		if err := ctx.Err(); err != nil {
			return
		}
		d, hint, proceed := m.drainCollection(ctx, coll, nowMs)
		if !proceed {
			// Context cancelled, or a busy/IO failure ended the sweep:
			// nothing about progress was persisted, so the next cycle or
			// reopen continues from the catalog.
			return
		}
		deleted += d
		if hint < earliest {
			earliest = hint
		}
	}
	return
}

// drainCollection removes expired documents of one collection in bounded
// transactions, looping while a batch filled up (a backlog may span many
// batches). It returns the total deleted, the earliest upcoming boundary
// seen, and proceed=false when the sweep must stop (cancelled context, or
// a busy/IO error the next cycle retries). A collection dropped or renamed
// since discovery drains as zero deletes.
func (m *ttlMonitor) drainCollection(ctx context.Context, coll string, nowMs int64) (deleted int, earliest int64, proceed bool) {
	earliest = math.MaxInt64
	for {
		var (
			n        int
			capped   bool
			firstDue int64 = math.MaxInt64
		)
		err := m.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			// Resolve the collection through the WRITER's view every
			// time: reuse the registered handle when the application has
			// the collection open (so its schema objects and vector
			// caches are shared with normal writes), otherwise load a
			// private throwaway schema for this transaction. A
			// collection dropped or renamed by a peer (or by this
			// process's own committed DDL) simply fails to resolve and
			// is skipped.
			c, s, cErr := m.resolveSweepCollection(&tx.ReadTx, coll)
			if cErr != nil {
				return cErr
			}
			buf := m.db.syncPool.GetDocBuf()
			defer m.db.syncPool.ReleaseDocBuf(buf)
			for _, idx := range s.indexes {
				if idx.info.TTL == nil {
					continue
				}
				full, hint, sErr := sweepTTLIndex(tx, c, s, idx, nowMs, buf, &n)
				if sErr != nil {
					return sErr
				}
				if hint < firstDue {
					firstDue = hint
				}
				if full {
					capped = true
					break
				}
			}
			return nil
		})
		switch {
		case err == nil:
			deleted += n
			if firstDue < earliest {
				earliest = firstDue
			}
			// Another batch only while the last one filled up; an
			// unfinished batch (or zero deletes) means this collection
			// is drained for now.
			if !capped {
				return deleted, earliest, true
			}
		case errors.Is(err, ErrCollectionNotFound), errors.Is(err, ErrCollectionClosed):
			// Dropped or renamed since discovery; a later cycle uses the
			// current catalog name.
			return deleted, earliest, true
		case ctx.Err() != nil:
			return deleted, earliest, false
		default:
			// busy / busy snapshot / IO error / integrity error: the
			// batch transaction rolled back entirely (no half-deleted
			// indexes); the next cycle retries from the catalog.
			return deleted, earliest, false
		}
		if err = ctx.Err(); err != nil {
			return deleted, earliest, false
		}
	}
}

// resolveSweepCollection returns the collection and the schema version to
// sweep in tx's view: the application's registered handle when one is live
// (sharing its schema/index objects with user writes), otherwise a private
// throwaway handle loaded for this transaction only.
func (m *ttlMonitor) resolveSweepCollection(tx *btree.ReadTx, name string) (*collection, *collSchema, error) {
	if h, ok := m.db.registered(name); ok {
		c := h.(*collection)
		s, err := c.resolve(tx)
		if err == nil {
			return c, s, nil
		}
		// Closed/retired between the registry lookup and resolve: fall
		// through to a fresh load, which reports the catalog's truth.
		if !errors.Is(err, ErrCollectionClosed) && !errors.Is(err, ErrCollectionNotFound) {
			return nil, nil, err
		}
	}
	return newCollection(m.db, name, tx)
}

// sweepTTLIndex deletes at most ttlDeleteBatch documents expired through
// one TTL index in the open write transaction tx. It returns full=true
// when more expired documents may remain for another transaction, and the
// earliest non-expired DateTime boundary observed (math.MaxInt64 if
// none). Enumeration finishes and its cursor closes BEFORE any namespace
// is mutated, so deleting entries of this same index never moves a cursor
// that is still walking it.
func sweepTTLIndex(tx *btree.WriteTx, c *collection, s *collSchema, idx *index, nowMs int64, buf *syncpool.DocBuffer, deleted *int) (full bool, earliest int64, err error) {
	earliest = math.MaxInt64
	expireMs := idx.info.TTL.ExpireAfterMillis()
	// Inclusive cutoff: fieldMillis <= nowMillis-expireMs, i.e. the
	// boundary millisecond itself (field + expire == now) is deleted —
	// never earlier.
	cutoff := nowMs - expireMs
	var a anyenc.Arena
	bound := a.NewDateTimeMillis(cutoff).MarshalTo(nil)

	cursor := tx.NewCursor(idx.ns)
	var (
		ids [][]byte
		p   anyenc.Parser
	)
	if err = cursor.First(); err != nil {
		cursor.Close()
		return false, earliest, err
	}
	for cursor.Valid() {
		var key []byte
		key, err = cursor.Key()
		if err != nil {
			break
		}
		var field []byte
		field, err = anyenc.Tuple(key).FieldBytes(0)
		if err != nil {
			break
		}
		if field[0] != byte(anyenc.TypeDateTime) {
			// Every non-DateTime key sorts before DateTime keys (their
			// type tag is smaller): missing/null/string/... values are
			// indexed there and are non-expiring by definition, so they
			// are skipped but never stop the walk.
			if err = cursor.Next(); err != nil {
				break
			}
			continue
		}
		if bytes.Compare(field, bound) > 0 {
			// First live key past the cutoff: DateTime keys are ordered
			// chronologically, so it carries the earliest upcoming
			// boundary of this index.
			if fv, pErr := p.Parse(field); pErr == nil {
				if ms, dErr := fv.DateTimeMillis(); dErr == nil {
					earliest = ms + expireMs
				}
			}
			break
		}
		var off int
		off, err = anyenc.Tuple(key).OffsetAfter(1)
		if err != nil {
			break
		}
		// Copy: cursor-owned bytes must not be retained past Next/Close.
		ids = append(ids, append([]byte(nil), key[off:]...))
		if len(ids) >= ttlDeleteBatch {
			full = true
			break
		}
		if err = cursor.Next(); err != nil {
			break
		}
	}
	cursor.Close()
	if err != nil {
		return false, earliest, err
	}

	// Enumeration is done; only now mutate namespaces. Every deletion
	// takes the ordinary document deletion path, which removes the data
	// record and the entries of every range, full-text and vector index
	// in this same transaction.
	for _, id := range ids {
		var it item
		it, err = c.loadById(tx, s, buf, id)
		if err != nil {
			if errors.Is(err, ErrDocNotFound) {
				// Deleted in a committed transaction after this tx's
				// snapshot began — impossible for same-process writers
				// (single write lock), possible across processes when
				// the writer retried; nothing to do either way.
				err = nil
				continue
			}
			return false, earliest, err
		}
		// The document field is the authority, re-checked against the
		// writer's view: a concurrently committed update moves or clears
		// the boundary, and the deletion decision always follows the
		// CURRENT field value plus the duration.
		if !ttlDocExpired(it.Value(), idx.fieldPaths[0], expireMs, nowMs) {
			continue
		}
		if err = c.deleteItem(tx, s, buf, id); err != nil {
			return false, earliest, err
		}
		*deleted++
	}
	return full, earliest, nil
}

// ttlDocExpired reports whether doc carries at the field path a DateTime
// value with fieldMillis + expireMs <= nowMs. The walk mirrors the single
// ascending field index's key generation: a path through an array fans out
// over the elements, and an array leaf flattens one level; a missing, null
// or non-DateTime leaf never expires. So the predicate matches the index
// scan candidate set exactly at create, reopen and rebuild.
func ttlDocExpired(doc *anyenc.Value, path []string, expireMs, nowMs int64) bool {
	return ttlWalkExpired(doc, path, 0, expireMs, nowMs)
}

func ttlWalkExpired(v *anyenc.Value, path []string, seg int, expireMs, nowMs int64) bool {
	if v == nil {
		return false
	}
	if seg >= len(path) {
		return ttlLeafExpired(v, expireMs, nowMs)
	}
	switch v.Type() {
	case anyenc.TypeObject:
		return ttlWalkExpired(v.Get(path[seg]), path, seg+1, expireMs, nowMs)
	case anyenc.TypeArray:
		arr, _ := v.Array()
		if n, ok := anyenc.ParseIndexSegment(path[seg]); ok {
			if n < 0 || n >= len(arr) {
				return false
			}
			return ttlWalkExpired(arr[n], path, seg+1, expireMs, nowMs)
		}
		for _, el := range arr {
			if ttlWalkExpired(el, path, seg, expireMs, nowMs) {
				return true
			}
		}
		return false
	default:
		// A non-container where an object/array step is expected: the
		// leaf is missing for the rest of the path and never expires.
		return false
	}
}

func ttlLeafExpired(v *anyenc.Value, expireMs, nowMs int64) bool {
	switch v.Type() {
	case anyenc.TypeDateTime:
		ms, err := v.DateTimeMillis()
		if err != nil {
			return false
		}
		return ms <= nowMs-expireMs
	case anyenc.TypeArray:
		arr, _ := v.Array()
		for _, el := range arr {
			if ttlLeafExpired(el, expireMs, nowMs) {
				return true
			}
		}
		return false
	default:
		return false
	}
}
