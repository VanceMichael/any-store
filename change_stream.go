package anystore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

// OperationType is the kind of change a ChangeEvent describes.
type OperationType uint8

const (
	// OperationInsert is emitted for a committed document insert.
	OperationInsert OperationType = 1
	// OperationUpdate is emitted for a committed document update.
	OperationUpdate OperationType = 2
	// OperationDelete is emitted for a committed document delete.
	OperationDelete OperationType = 3
)

// ChangeEvent is one committed insert, update or delete.
type ChangeEvent struct {
	// Operation is the kind of the committed change.
	Operation OperationType

	// Key is the raw encoded primary-key value of the changed document,
	// a copy owned by the stream.
	Key anyenc.Tuple

	// Doc is the full document after the change, for insert and update,
	// parsed from the exact bytes committed to the data namespace (the
	// compressed form is self-describing and decoded transparently).
	// Valid only until the next call to Next, like an iterator value.
	// nil for delete.
	Doc *anyenc.Value

	// ResumeToken is the monotonic position of this event. Save it once
	// the change is durably handled; resuming from it replays only the
	// events AFTER it — this event is never delivered twice.
	ResumeToken ResumeToken
}

// ChangeStream watches the committed changes of one collection, identified
// by its immutable catalog identity (not its current name). A rename does
// not interrupt the stream; a drop ends it with ErrChangeStreamEnded; a
// same-named recreated collection is a different stream.
type ChangeStream interface {
	// Next returns the next committed change. It blocks while the stream
	// is caught up, honoring ctx: a cancelled or timed-out ctx returns its
	// error without advancing the position, so the next call retries.
	Next(ctx context.Context) (ChangeEvent, error)

	// Close ends the stream and unblocks a waiting Next (ErrChangeStreamClosed).
	// Idempotent.
	Close() error
}

// WatchStart selects where a stream without a ResumeToken begins.
type WatchStart uint8

const (
	// WatchStartBeginning replays from the oldest retained event (default).
	WatchStartBeginning WatchStart = iota
	// WatchStartNow begins after the newest event present at Watch time.
	WatchStartNow
)

// WatchOptions configures a change stream.
type WatchOptions struct {
	// ResumeToken resumes a previously saved position: only events after
	// it are replayed. When set, StartFrom is ignored.
	ResumeToken ResumeToken

	// StartFrom selects the start without a ResumeToken
	// (default WatchStartBeginning).
	StartFrom WatchStart

	// BatchSize bounds how many events one polling read transaction
	// materializes (default 256).
	BatchSize int
}

// ---------------------------------------------------------------------------
// Sentinel errors
// ---------------------------------------------------------------------------

var (
	// ErrChangeStreamEnded is returned by Next after the collection was
	// dropped: the old stream ends explicitly. Events already buffered
	// before the drop are delivered first.
	ErrChangeStreamEnded = errors.New("any-store: change stream ended (collection dropped)")

	// ErrChangeStreamClosed is returned by Next after Close was called.
	ErrChangeStreamClosed = errors.New("any-store: change stream closed")

	// ErrInvalidResumeToken is returned when a resume token is malformed,
	// belongs to another collection identity (e.g. a same-named collection
	// recreated after a drop), or names a position beyond the stream.
	ErrInvalidResumeToken = errors.New("any-store: invalid change stream resume token")

	// ErrChangeHistoryLost is returned when events at and before the
	// resume position have been purged and the position is no longer a
	// valid anchor. The subscriber cannot be safely resumed there.
	ErrChangeHistoryLost = errors.New("any-store: change history before the resume token has been purged")
)

// ---------------------------------------------------------------------------
// Resume token
// ---------------------------------------------------------------------------

const resumeTokenVersion byte = 1

// ResumeToken is the savable, opaque position of a change event: version
// byte + length-prefixed catalog token + 8-byte sequence. The embedded
// catalog token binds it to one collection identity, so a token resumes
// only the stream it came from — including across renames, and excluding a
// same-named collection recreated after a drop.
type ResumeToken []byte

func newResumeToken(catalogToken []byte, seq uint64) ResumeToken {
	b := make([]byte, 0, 1+2+len(catalogToken)+8)
	b = append(b, resumeTokenVersion)
	b = binary.BigEndian.AppendUint16(b, uint16(len(catalogToken)))
	b = append(b, catalogToken...)
	b = binary.BigEndian.AppendUint64(b, seq)
	return b
}

// Decode unpacks the catalog token and sequence of the resume token.
func (t ResumeToken) Decode() (catalogToken []byte, seq uint64, err error) {
	if len(t) < 1+2+8 {
		return nil, 0, ErrInvalidResumeToken
	}
	if t[0] != resumeTokenVersion {
		return nil, 0, fmt.Errorf("%w: unknown token version", ErrInvalidResumeToken)
	}
	n := int(binary.BigEndian.Uint16(t[1:]))
	end := 3 + n
	if n < 0 || end+8 != len(t) {
		return nil, 0, ErrInvalidResumeToken
	}
	return t[3:end], binary.BigEndian.Uint64(t[end:]), nil
}

// Seq returns the sequence of the token (0 when it cannot be decoded).
func (t ResumeToken) Seq() uint64 {
	_, seq, err := t.Decode()
	if err != nil {
		return 0
	}
	return seq
}

// ---------------------------------------------------------------------------
// Per-transaction change buffer (writer-owned, lives in txSchema)
// ---------------------------------------------------------------------------

// pendingChange is an uncommitted, successfully applied write awaiting
// sequence assignment at commit. All slices are copies independent of
// pooled buffers and user-owned values.
type pendingChange struct {
	op   OperationType
	coll *collection
	id   []byte
	doc  []byte // stored-form document bytes; nil for delete
}

// changeBuffer collects the transaction's changes in execution order.
// The same determinism that serializes the verbs in one write tx gives the
// events their order; sequence numbers are assigned at commit.
type changeBuffer struct {
	pending []pendingChange
}

func (b *changeBuffer) add(p pendingChange) {
	b.pending = append(b.pending, p)
}

func (b *changeBuffer) mark() int { return len(b.pending) }

// rollbackTo discards changes after mark (a savepoint rollback): their
// writes were reverted by the btree and no event may survive them.
func (b *changeBuffer) rollbackTo(mark int) {
	if mark < 0 || mark > len(b.pending) {
		return
	}
	clear(b.pending[mark:])
	b.pending = b.pending[:mark]
}

func (b *changeBuffer) reset() {
	clear(b.pending)
	b.pending = b.pending[:0]
}

// ---------------------------------------------------------------------------
// In-process commit notifier (cross-process still via WAL-index polling)
// ---------------------------------------------------------------------------

// changeNotifier wakes caught-up streams after a commit of THIS process.
// It carries no cross-process semantics: peer commits are observed by
// polling the mmap WAL-index, the database's existing visibility channel.
type changeNotifier struct {
	mu      sync.Mutex
	signal  chan struct{} // closed to broadcast, then replaced
	waiters int
}

func newChangeNotifier() *changeNotifier {
	return &changeNotifier{signal: make(chan struct{})}
}

func (n *changeNotifier) waitCh() (<-chan struct{}, func()) {
	n.mu.Lock()
	ch := n.signal
	n.waiters++
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		n.waiters--
		n.mu.Unlock()
	}
}

// wake broadcasts to waiting streams. No-op without waiters: a stream
// registers before it waits, so it can never miss the next wake.
func (n *changeNotifier) wake() {
	n.mu.Lock()
	if n.waiters == 0 {
		n.mu.Unlock()
		return
	}
	close(n.signal)
	n.signal = make(chan struct{})
	n.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Stream namespace layout
// ---------------------------------------------------------------------------

const changeStreamNsPrefix = "csx:"

// streamMetaKey is the 1-byte key of the stream metadata, always before
// the 8-byte event keys (shorter key with equal prefix sorts first).
var streamMetaKey = []byte{0}

type streamMeta struct {
	first uint64 // oldest retained sequence
	last  uint64 // newest sequence
}

// legacySharedCatalogToken is the catalog value every collection shares in
// files created before random catalog IDs. Such tokens are disambiguated by
// the data root page when deriving the stream namespace name.
var legacySharedCatalogToken = []byte("1")

// catalogTokenBytes returns the raw catalog token of the handle.
func (c *collection) catalogTokenBytes() []byte {
	if len(c.identity) <= 4 {
		return nil
	}
	return []byte(c.identity[:len(c.identity)-4])
}

// changeStreamNsName derives the stream namespace name solely from the
// immutable catalog identity: unchanged by rename, deleted with drop,
// unique for a same-named recreated collection (which gets a new token).
func changeStreamNsName(c *collection) string {
	token := c.catalogTokenBytes()
	name := changeStreamNsPrefix + hex.EncodeToString(token)
	if bytes.Equal(token, legacySharedCatalogToken) {
		// Old files share one token across collections: suffix the root.
		name += "_" + fmt.Sprintf("%x", c.dataRoot)
	}
	return name
}

func seqKey(seq uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, seq)
}

func encodeEvent(p pendingChange) []byte {
	var a anyenc.Arena
	obj := a.NewObject()
	obj.Set("op", a.NewNumberInt(int(p.op)))
	obj.Set("id", a.NewBinary(p.id))
	if p.doc != nil {
		obj.Set("d", a.NewBinary(p.doc))
	}
	return obj.MarshalTo(nil)
}

func decodeEvent(raw []byte) (op OperationType, id, doc []byte, err error) {
	var p anyenc.Parser
	v, err := p.Parse(raw)
	if err != nil {
		return 0, nil, nil, err
	}
	op = OperationType(v.GetInt("op"))
	id = v.GetBytes("id")
	if d := v.GetBytes("d"); len(d) > 0 {
		doc = d
	}
	if op < OperationInsert || op > OperationDelete || len(id) == 0 {
		return 0, nil, nil, fmt.Errorf("%w: malformed event record", ErrInvalidResumeToken)
	}
	return op, id, doc, nil
}

func readStreamMeta(tx *btree.ReadTx, ns *btree.Namespace) (m streamMeta, err error) {
	raw, err := tx.AppendValue(ns, streamMetaKey, nil)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return streamMeta{}, nil
		}
		return streamMeta{}, err
	}
	var p anyenc.Parser
	v, err := p.Parse(raw)
	if err != nil {
		return streamMeta{}, nil
	}
	return streamMeta{first: uint64(v.GetInt("first")), last: uint64(v.GetInt("last"))}, nil
}

func writeStreamMeta(tx *btree.WriteTx, ns *btree.Namespace, m streamMeta) error {
	var a anyenc.Arena
	obj := a.NewObject()
	obj.Set("first", a.NewNumberInt(int(m.first)))
	obj.Set("last", a.NewNumberInt(int(m.last)))
	return tx.Put(ns, streamMetaKey, obj.MarshalTo(nil))
}

// ---------------------------------------------------------------------------
// Commit-time event materialization (runs inside writeTx.Commit, same tx)
// ---------------------------------------------------------------------------

// streamState is one collection's stream during a commit.
type streamState struct {
	ns    *btree.Namespace
	first uint64
	last  uint64
}

// prepareChangeEvents assigns monotonic sequence numbers to the buffered
// changes and writes them, with the updated metadata, into the SAME btree
// write transaction as the documents and the range/FTS/vector indexes — so
// events and data become visible atomically at its commit.
// On failure the caller rolls the whole btree tx back: nothing is published.
func (db *db) prepareChangeEvents(tx *btree.WriteTx, b *changeBuffer) error {
	if len(b.pending) == 0 {
		return nil
	}
	states := make(map[*collection]*streamState)
	for i := range b.pending {
		p := &b.pending[i]
		st, ok := states[p.coll]
		if !ok {
			nsName := changeStreamNsName(p.coll)
			ns, err := tx.GetNamespace(nsName)
			if err != nil {
				if !errors.Is(err, btree.ErrNamespaceNotFound) {
					return err
				}
				// First change for this collection: create the stream in-tx.
				ns, err = tx.CreateNamespace(nsName)
				if err != nil {
					return err
				}
			}
			meta, err := readStreamMeta(&tx.ReadTx, ns)
			if err != nil {
				return err
			}
			first := meta.first
			if first == 0 {
				first = 1
			}
			st = &streamState{ns: ns, first: first, last: meta.last}
			states[p.coll] = st
		}
		st.last++
		if err := tx.Put(st.ns, seqKey(st.last), encodeEvent(*p)); err != nil {
			return err
		}
	}
	for _, st := range states {
		if err := writeStreamMeta(tx, st.ns, streamMeta{first: st.first, last: st.last}); err != nil {
			return err
		}
	}
	b.reset()
	return nil
}

// ---------------------------------------------------------------------------
// Watch
// ---------------------------------------------------------------------------

// Watch opens a change stream on the collection identified by this handle.
// The identity — not the name — defines the stream: a later rename keeps
// the stream and every token valid.
func (c *collection) Watch(ctx context.Context, opts ...WatchOptions) (ChangeStream, error) {
	if err := c.alive(); err != nil {
		return nil, err
	}
	var opt WatchOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = 256
	}
	s := &changeStream{
		db:   c.db,
		coll: c,
		opts: opt,
		done: make(chan struct{}),
	}
	if opt.ResumeToken != nil {
		ct, seq, err := opt.ResumeToken.Decode()
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(ct, c.catalogTokenBytes()) {
			return nil, fmt.Errorf("%w: token belongs to another collection", ErrInvalidResumeToken)
		}
		s.nextSeq = seq + 1
		s.token = opt.ResumeToken
		return s, nil
	}
	if opt.StartFrom == WatchStartNow {
		// Resolve the newest position at Watch time; absent stream → 0.
		err := c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
			ns, err := tx.GetNamespace(changeStreamNsName(c))
			if err != nil {
				if errors.Is(err, btree.ErrNamespaceNotFound) {
					return nil
				}
				return err
			}
			meta, err := readStreamMeta(tx, ns)
			if err != nil {
				return err
			}
			s.nextSeq = meta.last + 1
			s.token = newResumeToken(c.catalogTokenBytes(), meta.last)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	// WatchStartBeginning: nextSeq 0 → the first poll starts at meta.first.
	return s, nil
}

// WatchCollection opens a change stream on the named collection.
func (db *db) WatchCollection(ctx context.Context, collectionName string, opts ...WatchOptions) (ChangeStream, error) {
	coll, err := db.openCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}
	return coll.(*collection).Watch(ctx, opts...)
}

// ---------------------------------------------------------------------------
// changeStream
// ---------------------------------------------------------------------------

type bufferedEvent struct {
	op  OperationType
	id  []byte
	doc []byte
	seq uint64
}

type changeStream struct {
	db   *db
	coll *collection
	opts WatchOptions

	mu     sync.Mutex
	closed bool
	done   chan struct{}

	// batch are events of the last poll not yet delivered; token only
	// advances as they leave, so an undelivered event is never confirmed.
	batch []bufferedEvent

	// token is the last confirmed position (nil before the first event).
	token ResumeToken
	// nextSeq is where the next poll seeks; 0 → start at meta.first.
	nextSeq uint64

	parser anyenc.Parser
}

func (s *changeStream) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.done)
	}
	s.mu.Unlock()
	return nil
}

func (s *changeStream) Next(ctx context.Context) (e ChangeEvent, err error) {
	// Buffered events are delivered regardless of ctx state: their read
	// already happened, advancing to them loses nothing.
	for len(s.batch) == 0 {
		select {
		case <-s.done:
			return ChangeEvent{}, ErrChangeStreamClosed
		default:
		}
		ended, perr := s.poll(ctx)
		if perr != nil {
			// Read failure/cancel: position untouched; the tx is released
			// inside poll, and the next call retries from the same token.
			return ChangeEvent{}, perr
		}
		if ended {
			_ = s.Close()
			return ChangeEvent{}, ErrChangeStreamEnded
		}
		if len(s.batch) == 0 {
			if werr := s.wait(ctx); werr != nil {
				return ChangeEvent{}, werr
			}
		}
	}
	ev := s.batch[0]
	s.batch = s.batch[1:]

	token := newResumeToken(s.coll.catalogTokenBytes(), ev.seq)
	s.token = token
	s.nextSeq = ev.seq + 1

	ce := ChangeEvent{
		Operation:   ev.op,
		Key:         ev.id,
		ResumeToken: token,
	}
	if len(ev.doc) > 0 {
		doc, derr := s.parser.Parse(ev.doc)
		if derr != nil {
			return ChangeEvent{}, derr
		}
		ce.Doc = doc
	}
	return ce, nil
}

// wait blocks until an in-process commit wakes the stream, the poll
// interval elapses (peer commits become visible via the WAL-index), ctx
// is cancelled, or the stream is closed.
func (s *changeStream) wait(ctx context.Context) error {
	ch, release := s.db.streamNotifier.waitCh()
	defer release()

	interval := s.db.config.ChangeStreamPollInterval
	var tc <-chan time.Time
	if interval > 0 {
		timer := time.NewTimer(interval)
		defer timer.Stop()
		tc = timer.C
	}
	select {
	case <-ch:
		return nil
	case <-tc:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrChangeStreamClosed
	}
}

// poll runs one bounded read: fresh read tx at the newest committed
// snapshot, resolve the collection (re-homing across a peer rename),
// materialize at most BatchSize events after the confirmed position,
// then release the tx BEFORE the events are delivered. Returns ended
// when the collection identity has vanished from the catalog (dropped).
func (s *changeStream) poll(ctx context.Context) (ended bool, err error) {
	if err = ctx.Err(); err != nil {
		return false, err
	}
	tx, err := s.db.ReadTx(ctx)
	if err != nil {
		return false, err
	}
	bt := tx.btreeReadTx()

	coll := s.coll
	if _, rErr := coll.resolve(bt); rErr != nil {
		// Current handle no longer answers: either dropped, or renamed by
		// another process (which retires the old handle). Re-resolve by
		// the immutable catalog token before declaring the end.
		c2, iErr := s.db.findByIdentity(bt, tx, coll.catalogTokenBytes(), coll.dataRoot)
		if iErr != nil {
			_ = tx.Commit()
			if errors.Is(iErr, errIdentityNotFound) {
				return true, nil
			}
			return false, iErr
		}
		coll = c2
		s.coll = c2
	}

	ns, err := bt.GetNamespace(changeStreamNsName(coll))
	if err != nil {
		_ = tx.Commit()
		if errors.Is(err, btree.ErrNamespaceNotFound) {
			// Live collection with no events written yet.
			return false, nil
		}
		return false, err
	}
	meta, err := readStreamMeta(bt, ns)
	if err != nil {
		_ = tx.Commit()
		return false, err
	}
	start := s.nextSeq
	if start == 0 {
		// No position yet (beginning): oldest retained event.
		start = meta.first
		if start == 0 {
			start = 1
		}
		s.nextSeq = start
	}
	if start < meta.first {
		// The event before start was purged while this stream held a
		// token below the new anchor: history is unrecoverable.
		_ = tx.Commit()
		return false, ErrChangeHistoryLost
	}
	if start > meta.last {
		// Caught up (or a future position): nothing at this snapshot.
		_ = tx.Commit()
		return false, nil
	}

	cursor := bt.NewCursor(ns)
	if err = cursor.Seek(seqKey(start)); err != nil {
		_ = tx.Commit()
		return false, err
	}
	batch := make([]bufferedEvent, 0, s.opts.BatchSize)
	for cursor.Valid() && len(batch) < cap(batch) {
		key, kerr := cursor.Key()
		if kerr != nil {
			err = kerr
			break
		}
		if len(key) != 8 {
			err = fmt.Errorf("%w: event key is not 8 bytes", ErrInvalidResumeToken)
			break
		}
		val, verr := cursor.Value()
		if verr != nil {
			err = verr
			break
		}
		op, id, doc, derr := decodeEvent(val)
		if derr != nil {
			err = derr
			break
		}
		batch = append(batch, bufferedEvent{
			op:  op,
			id:  append([]byte(nil), id...),
			doc: append([]byte(nil), doc...),
			seq: binary.BigEndian.Uint64(key),
		})
		if nerr := cursor.Next(); nerr != nil {
			err = nerr
			break
		}
	}
	cursor.Close()
	// Release the read tx before returning: delivered events own copies.
	if cerr := tx.Commit(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return false, err
	}
	if len(batch) > 0 {
		// The first materialized sequence must be exactly where we seeked:
		// a gap would silently skip changes.
		if batch[0].seq != start {
			return false, fmt.Errorf("%w: expected event %d, found %d (gap)", ErrInvalidResumeToken, start, batch[0].seq)
		}
		s.batch = batch
	}
	return false, nil
}

// errIdentityNotFound marks a catalog identity scan with no match: the
// collection was dropped (no rename target with the same catalog token).
var errIdentityNotFound = errors.New("any-store: collection identity not found in catalog")

// findByIdentity scans catalog records through tx for the given catalog
// token (legacy shared tokens additionally require the same data root),
// and returns the live handle of the matching collection — re-homing a
// stream after another process renamed the collection.
func (db *db) findByIdentity(bt *btree.ReadTx, ambient ReadTx, token []byte, root uint32) (*collection, error) {
	cursor := bt.NewCursor(db.systemNS)
	defer cursor.Close()
	if err := cursor.Seek([]byte("coll:")); err != nil {
		return nil, err
	}
	legacy := bytes.Equal(token, legacySharedCatalogToken)
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(string(key), "coll:") {
			break
		}
		val, err := cursor.Value()
		if err != nil {
			return nil, err
		}
		if bytes.Equal(val, token) {
			name := string(key[len("coll:"):])
			if legacy {
				ns, nerr := bt.GetNamespace(name)
				if nerr != nil {
					return nil, nerr
				}
				if ns.RootPage() != root {
					if nerr = cursor.Next(); nerr != nil {
						return nil, nerr
					}
					continue
				}
			}

			// Resolve the handle through the ambient snapshot (no new tx).
			ctx := context.WithValue(context.Background(), ctxKeyTx, ambient)
			coll, oerr := db.openCollection(ctx, name)
			if oerr != nil {
				return nil, oerr
			}
			return coll.(*collection), nil
		}
		if err = cursor.Next(); err != nil {
			return nil, err
		}
	}
	return nil, errIdentityNotFound
}

// ---------------------------------------------------------------------------
// Purge
// ---------------------------------------------------------------------------

// PurgeChangeEvents deletes the events at and before the token position
// and advances the retention anchor to token+1. Events after the token —
// unconfirmed or in a replay window — are never deleted. The token stays
// a valid resume anchor; tokens before it then fail with
// ErrChangeHistoryLost. The caller must ensure every durable subscriber
// has confirmed the token position.
func (c *collection) PurgeChangeEvents(ctx context.Context, token ResumeToken) error {
	ct, seq, err := token.Decode()
	if err != nil {
		return err
	}
	if !bytes.Equal(ct, c.catalogTokenBytes()) {
		return fmt.Errorf("%w: token belongs to another collection", ErrInvalidResumeToken)
	}
	return c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		if _, rErr := c.resolve(&tx.ReadTx); rErr != nil {
			return rErr
		}
		ns, err := tx.GetNamespace(changeStreamNsName(c))
		if err != nil {
			if errors.Is(err, btree.ErrNamespaceNotFound) {
				return nil
			}
			return err
		}
		meta, err := readStreamMeta(&tx.ReadTx, ns)
		if err != nil {
			return err
		}
		// Nothing retained at or before seq.
		if seq < meta.first {
			if seq == meta.first-1 {
				return nil // purge at the current anchor: no-op
			}
			return ErrChangeHistoryLost
		}
		if seq > meta.last {
			return fmt.Errorf("%w: token sequence %d is beyond the stream (last %d)",
				ErrInvalidResumeToken, seq, meta.last)
		}
		// Collect the keys first: deleting under a cursor restructures
		// the pages it walks.
		var keys [][]byte
		cursor := tx.NewCursor(ns)
		if err = cursor.Seek(seqKey(meta.first)); err != nil {
			return err
		}
		for cursor.Valid() {
			k, kerr := cursor.Key()
			if kerr != nil {
				return kerr
			}
			if len(k) != 8 || binary.BigEndian.Uint64(k) > seq {
				break
			}
			keys = append(keys, append([]byte(nil), k...))
			if nerr := cursor.Next(); nerr != nil {
				return nerr
			}
		}
		cursor.Close()
		for _, k := range keys {
			if err = tx.Delete(ns, k); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
				return err
			}
		}
		return writeStreamMeta(tx, ns, streamMeta{first: seq + 1, last: meta.last})
	})
}
