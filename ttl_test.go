package anystore

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// ttlFixtureAt opens a database with a long sweep interval (the background
// loop never interferes) and pins its clock before any TTL index can exist,
// so the pin happens-before the monitor goroutine's first read of it.
func ttlFixtureAt(t *testing.T, nowMs int64) (*fixture, *db) {
	fx := newFixture(t, &Config{TTL: TTLConfig{SweepInterval: time.Hour}})
	ds := fx.DB.(*db)
	ds.ttl.suspendAuto = true
	pinTTLClock(ds, nowMs)
	return fx, ds
}

func ttlDoc(a *anyenc.Arena, id string, tsMs int64) *anyenc.Value {
	o := a.NewObject()
	o.Set("id", a.NewString(id))
	o.Set("ts", a.NewDateTimeMillis(tsMs))
	return o
}

func pinTTLClock(ds *db, ms int64) {
	ds.ttl.nowMilli = func() int64 { return ms }
}

// ttlIndexInfo is a TTL index definition used across the tests.
func ttlIndexInfo(expire time.Duration) IndexInfo {
	return IndexInfo{Name: "ttl_ts", Fields: []string{"ts"}, TTL: &TTLParams{ExpireAfter: expire}}
}

func TestTTL_ValidateDefinition(t *testing.T) {
	fx, _ := ttlFixtureAt(t, 0)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)

	cases := []struct {
		name string
		info IndexInfo
	}{
		{"compound", IndexInfo{Fields: []string{"ts", "x"}, TTL: &TTLParams{ExpireAfter: time.Second}}},
		{"descending", IndexInfo{Fields: []string{"-ts"}, TTL: &TTLParams{ExpireAfter: time.Second}}},
		{"negative", IndexInfo{Fields: []string{"ts"}, TTL: &TTLParams{ExpireAfter: -time.Millisecond}}},
		{"fulltext", IndexInfo{Kind: IndexKindFulltext, Fields: []string{"ts"}, TTL: &TTLParams{ExpireAfter: time.Second}}},
		{"vector", IndexInfo{Kind: IndexKindVector, Fields: nil, Vector: &VectorParams{Field: "v", Dim: 2}, TTL: &TTLParams{ExpireAfter: time.Second}}},
		{"no field", IndexInfo{Fields: nil, TTL: &TTLParams{ExpireAfter: time.Second}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorIs(t, coll.CreateIndex(ctx, tc.info), ErrTTLIndexInvalid)
		})
	}

	// Zero and positive durations are accepted; the field's VALUE type is
	// not validated (non-DateTime values simply never expire).
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "ok0", Fields: []string{"a"}, TTL: &TTLParams{ExpireAfter: 0}}))
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "ok1", Fields: []string{"b"}, TTL: &TTLParams{ExpireAfter: time.Millisecond}}))
}

func TestTTL_PersistedInSchemaAndReopened(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ttl.db")
	cfg := &Config{TTL: TTLConfig{SweepInterval: time.Hour}}

	db1, err := Open(ctx, path, cfg)
	require.NoError(t, err)
	coll, err := db1.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(1500*time.Millisecond)))

	var found bool
	for _, ix := range coll.GetIndexes() {
		if ix.Info().Name == "ttl_ts" {
			require.NotNil(t, ix.Info().TTL)
			assert.Equal(t, int64(1500), ix.Info().TTL.ExpireAfterMillis())
			found = true
		}
	}
	require.True(t, found)
	require.NoError(t, db1.Close())

	db2, err := Open(ctx, path, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	coll2, err := db2.OpenCollection(ctx, "c")
	require.NoError(t, err)
	for _, ix := range coll2.GetIndexes() {
		if ix.Info().Name == "ttl_ts" {
			require.NotNil(t, ix.Info().TTL)
			assert.Equal(t, int64(1500), ix.Info().TTL.ExpireAfterMillis())
		}
	}
	// Identical redefinition is idempotent; a changed duration conflicts.
	require.NoError(t, coll2.EnsureIndex(ctx, ttlIndexInfo(1500*time.Millisecond)))
	err = coll2.EnsureIndex(ctx, ttlIndexInfo(2*time.Second))
	require.ErrorIs(t, err, ErrIndexMismatch)
}

func TestTTL_BoundaryMillis(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))

	var a anyenc.Arena
	docs := []*anyenc.Value{
		ttlDoc(&a, "d8999", 8999), // boundary 9999  < 10000 -> expired
		ttlDoc(&a, "d9000", 9000), // boundary 10000 == now   -> expired AT the boundary
		ttlDoc(&a, "d9001", 9001), // boundary 10001 > now    -> alive
		func() *anyenc.Value { // non-DateTime value never expires
			o := a.NewObject()
			o.Set("id", a.NewString("dstr"))
			o.Set("ts", a.NewString("2020-01-01"))
			return o
		}(),
		func() *anyenc.Value { // explicit null never expires
			o := a.NewObject()
			o.Set("id", a.NewString("dnull"))
			o.Set("ts", a.NewNull())
			return o
		}(),
		func() *anyenc.Value { // missing field never expires
			o := a.NewObject()
			o.Set("id", a.NewString("dmiss"))
			return o
		}(),
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	ds.ttl.sweepOnce(ctx, []string{"c"}, now)

	count, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count)
	for _, id := range []string{"d8999", "d9000"} {
		_, err = coll.FindId(ctx, id)
		assert.ErrorIs(t, err, ErrDocNotFound, id)
	}
	for _, id := range []string{"d9001", "dstr", "dnull", "dmiss"} {
		_, err = coll.FindId(ctx, id)
		assert.NoError(t, err, id)
	}
	// New range queries (planner scans the TTL index itself) no longer
	// return the expired documents; non-DateTime values never match a
	// DateTime range.
	rangeCnt, err := coll.Find(`{"ts":{"$gte":{"$date":0}}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, rangeCnt, "only d9001 survives the DateTime range")

	// One millisecond before the boundary the document stays; the boundary
	// itself deletes it.
	ds.ttl.sweepOnce(ctx, []string{"c"}, 10_000)
	c1, _ := coll.Count(ctx)
	assert.Equal(t, 4, c1)

	ds.ttl.sweepOnce(ctx, []string{"c"}, 10_001)
	c2, _ := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, c2)
	_, err = coll.FindId(ctx, "d9001")
	assert.ErrorIs(t, err, ErrDocNotFound)
}

func TestTTL_UpdateRecomputesBoundary(t *testing.T) {
	const now int64 = 20_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(10*time.Second)))

	var a anyenc.Arena
	require.NoError(t, coll.Insert(ctx, ttlDoc(&a, "d", 5_000))) // would be expired

	// Move the field into the future before the sweep: new time wins.
	a.Reset()
	upd := ttlDoc(&a, "d", 15_000) // boundary 25_000
	require.NoError(t, coll.UpdateOne(ctx, upd))
	ds.ttl.sweepOnce(ctx, []string{"c"}, now)
	_, err = coll.FindId(ctx, "d")
	require.NoError(t, err)

	// Moving it back past the boundary expires it on the next sweep.
	a.Reset()
	require.NoError(t, coll.UpdateOne(ctx, ttlDoc(&a, "d", 0)))
	ds.ttl.sweepOnce(ctx, []string{"c"}, now)
	_, err = coll.FindId(ctx, "d")
	assert.ErrorIs(t, err, ErrDocNotFound)
}

func TestTTL_ArrayAndDottedFanout(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ttl_a", Fields: []string{"meta.ts"}, TTL: &TTLParams{ExpireAfter: time.Second},
	}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ttl_arr", Fields: []string{"dates"}, TTL: &TTLParams{ExpireAfter: time.Second},
	}))

	var a anyenc.Arena

	obj := func(id string, build func(o *anyenc.Value)) *anyenc.Value {
		o := a.NewObject()
		o.Set("id", a.NewString(id))
		build(o)
		return o
	}
	numArr := func(vals ...int64) *anyenc.Value {
		arr := a.NewArray()
		for i, v := range vals {
			arr.SetArrayItem(i, a.NewDateTimeMillis(v))
		}
		return arr
	}
	objArr := func(vals ...int64) *anyenc.Value {
		arr := a.NewArray()
		for i, v := range vals {
			e := a.NewObject()
			e.Set("ts", a.NewDateTimeMillis(v))
			arr.SetArrayItem(i, e)
		}
		return arr
	}

	docs := []*anyenc.Value{
		obj("fan-expired", func(o *anyenc.Value) { o.Set("meta", objArr(8_000, 20_000)) }),
		obj("fan-alive", func(o *anyenc.Value) { o.Set("meta", objArr(9_001, 20_000)) }),
		obj("leaf-array-expired", func(o *anyenc.Value) { o.Set("dates", numArr(20_000, 1_000)) }),
		obj("leaf-array-alive", func(o *anyenc.Value) { o.Set("dates", numArr(9_001, 9_999)) }),
		obj("mixed-types", func(o *anyenc.Value) {
			arr := a.NewArray()
			arr.SetArrayItem(0, a.NewString("nope"))
			arr.SetArrayItem(1, a.NewNull())
			o.Set("dates", arr)
		}),
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	ds.ttl.sweepOnce(ctx, []string{"c"}, now)

	for _, id := range []string{"fan-alive", "leaf-array-alive", "mixed-types"} {
		_, err = coll.FindId(ctx, id)
		assert.NoError(t, err, id)
	}
	for _, id := range []string{"fan-expired", "leaf-array-expired"} {
		_, err = coll.FindId(ctx, id)
		assert.ErrorIs(t, err, ErrDocNotFound, id)
	}
}

func TestTTL_DerivedIndexesAndStatsConverge(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ft", Kind: IndexKindFulltext, Fields: []string{"body"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "v", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: 2, Mode: VectorModeBruteForce},
	}))

	var a anyenc.Arena
	mk := func(id string, ts int64, body string, vec []float64) *anyenc.Value {
		o := a.NewObject()
		o.Set("id", a.NewString(id))
		o.Set("ts", a.NewDateTimeMillis(ts))
		o.Set("body", a.NewString(body))
		arr := a.NewArray()
		for i, f := range vec {
			arr.SetArrayItem(i, a.NewNumberFloat64(f))
		}
		o.Set("v", arr)
		return o
	}
	require.NoError(t, coll.Insert(ctx,
		mk("old", 1_000, "alpha banana", []float64{1, 0}),
		mk("new", 9_999, "beta banana", []float64{0, 1}),
	))

	ds.ttl.sweepOnce(ctx, []string{"c"}, now)

	// Data tree, Count, full-text and vector results all converge in the
	// sweep transactions.
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	n, err := coll.Find(`{"$text":{"$search":"alpha"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	n, err = coll.Find(`{"$text":{"$search":"beta"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	n, err = coll.Find(`{"v":{"$knn":{"$query":[1,0],"$k":5}}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, st.DocCount)
	require.Len(t, st.FtsIndexes, 1)
	assert.Equal(t, 1, st.FtsIndexes[0].DocCount)
	// The TTL range index keeps only surviving entries.
	for _, is := range st.Indexes {
		if is.Name == "ttl_ts" {
			assert.Equal(t, 1, is.EntryCount)
		}
	}
}

func TestTTL_TwoIndexesDropOne(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "i1", Fields: []string{"t1"}, TTL: &TTLParams{ExpireAfter: time.Second}},
		IndexInfo{Name: "i2", Fields: []string{"t2"}, TTL: &TTLParams{ExpireAfter: time.Second}},
	))

	var a anyenc.Arena
	mk := func(id string, t1, t2 any) *anyenc.Value {
		o := a.NewObject()
		o.Set("id", a.NewString(id))
		switch v := t1.(type) {
		case int64:
			o.Set("t1", a.NewDateTimeMillis(v))
		}
		switch v := t2.(type) {
		case int64:
			o.Set("t2", a.NewDateTimeMillis(v))
		}
		return o
	}
	require.NoError(t, coll.Insert(ctx,
		mk("only-i1-expired", int64(1_000), int64(20_000)),
		mk("only-i2-expired", int64(20_000), int64(1_000)),
		mk("both-alive", int64(20_000), int64(20_000)),
	))

	require.NoError(t, coll.DropIndex(ctx, "i1"))
	ds.ttl.sweepOnce(ctx, []string{"c"}, now)

	_, err = coll.FindId(ctx, "only-i1-expired")
	require.NoError(t, err, "dropping the TTL index must resurrect no task: document survives")
	_, err = coll.FindId(ctx, "only-i2-expired")
	assert.ErrorIs(t, err, ErrDocNotFound)
	_, err = coll.FindId(ctx, "both-alive")
	assert.NoError(t, err)
}

func TestTTL_DropCollectionAndRename(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))
	var a anyenc.Arena
	require.NoError(t, coll.Insert(ctx, ttlDoc(&a, "d", 1_000)))

	// Dropping the collection makes a sweep for its name a no-op.
	require.NoError(t, coll.Drop(ctx))
	ds.ttl.sweepOnce(ctx, []string{"c", "ghost"}, now)

	// A renamed collection keeps expiring under the new catalog name.
	coll2, err := fx.CreateCollection(ctx, "c2")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, ttlIndexInfo(time.Second)))
	a.Reset()
	require.NoError(t, coll2.Insert(ctx, ttlDoc(&a, "d", 1_000)))
	require.NoError(t, coll2.Rename(ctx, "c3"))

	cols, err := ds.ttl.ttlCollections(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"c3"}, cols)
	ds.ttl.sweepOnce(ctx, cols, now)

	reopened, err := fx.OpenCollection(ctx, "c3")
	require.NoError(t, err)
	_, err = reopened.FindId(ctx, "d")
	assert.ErrorIs(t, err, ErrDocNotFound)
}

func TestTTL_MissedSweepOnReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ttl.db")
	cfg := &Config{TTL: TTLConfig{SweepInterval: time.Hour}}

	db1, err := Open(ctx, path, cfg)
	require.NoError(t, err)
	coll, err := db1.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))
	base := time.Now().UnixMilli()
	var a anyenc.Arena
	old := a.NewObject()
	old.Set("id", a.NewString("old"))
	old.Set("ts", a.NewDateTimeMillis(base-10_000))
	future := a.NewObject()
	future.Set("id", a.NewString("future"))
	future.Set("ts", a.NewDateTimeMillis(base+10_000))
	nonDt := a.NewObject()
	nonDt.Set("id", a.NewString("str"))
	nonDt.Set("ts", a.NewString("not-a-date"))
	require.NoError(t, coll.Insert(ctx, old, future, nonDt))
	require.NoError(t, db1.Close())

	// Reopen runs the opening sweep synchronously: missed deadlines are
	// gone by the time Open returns.
	db2, err := Open(ctx, path, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	coll2, err := db2.OpenCollection(ctx, "c")
	require.NoError(t, err)
	_, err = coll2.FindId(ctx, "old")
	assert.ErrorIs(t, err, ErrDocNotFound)
	for _, id := range []string{"future", "str"} {
		_, err = coll2.FindId(ctx, id)
		assert.NoError(t, err, id)
	}
}

func TestTTL_BacklogDrainedInBatches(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))

	const n = 2500 // 3 batches of ttlDeleteBatch (1000) + one empty pass
	docs := make([]*anyenc.Value, 0, 2*n)
	var a anyenc.Arena
	for i := 0; i < n; i++ {
		docs = append(docs, ttlDoc(&a, fmt.Sprintf("old%d", i), 1_000))
		docs = append(docs, ttlDoc(&a, fmt.Sprintf("new%d", i), 20_000))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	deleted, _ := ds.ttl.sweepOnce(ctx, []string{"c"}, now)
	assert.Equal(t, n, deleted)
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, n, cnt)

	// Nothing remains expired; a second sweep deletes zero.
	deleted, _ = ds.ttl.sweepOnce(ctx, []string{"c"}, now)
	assert.Equal(t, 0, deleted)
}

func TestTTL_ContextCancelLosesNothing(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))
	var a anyenc.Arena
	docs := make([]*anyenc.Value, 0, 100)
	for i := 0; i < 100; i++ {
		docs = append(docs, ttlDoc(&a, fmt.Sprintf("d%d", i), 1_000))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	deleted, _ := ds.ttl.sweepOnce(cancelled, []string{"c"}, now)
	assert.Equal(t, 0, deleted)
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, cnt, "cancelled sweep must not delete anything")

	// A later cycle continues from the catalog and loses no records.
	ds.ttl.sweepOnce(ctx, []string{"c"}, now)
	cnt, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, cnt)
}

func TestTTL_MonitorLifecycle(t *testing.T) {
	fx := newFixture(t, &Config{TTL: TTLConfig{SweepInterval: 5 * time.Millisecond}})
	ds := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)

	require.False(t, ds.ttl.isRunning(), "no TTL index: monitor must not run")
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))
	require.Eventually(t, ds.ttl.isRunning, time.Second, time.Millisecond,
		"creating a TTL index starts the monitor")

	// End-to-end: the loop removes expired documents on its own.
	base := time.Now().UnixMilli()
	var a anyenc.Arena
	o := a.NewObject()
	o.Set("id", a.NewString("old"))
	o.Set("ts", a.NewDateTimeMillis(base-5_000))
	surv := a.NewObject()
	surv.Set("id", a.NewString("future"))
	surv.Set("ts", a.NewDateTimeMillis(base+60_000))
	require.NoError(t, coll.Insert(ctx, o, surv))
	require.Eventually(t, func() bool {
		c, _ := coll.Count(ctx)
		return c == 1
	}, 2*time.Second, 10*time.Millisecond)
	_, err = coll.FindId(ctx, "future")
	require.NoError(t, err)

	// Dropping the last TTL index ends the loop.
	require.NoError(t, coll.DropIndex(ctx, "ttl_ts"))
	require.Eventually(t, func() bool { return !ds.ttl.isRunning() },
		2*time.Second, 10*time.Millisecond, "monitor exits with no TTL index left")
}

func TestTTL_DropRecreateDoesNotMisdelete(t *testing.T) {
	const now int64 = 10_000
	fx, ds := ttlFixtureAt(t, now)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	// Old 1s definition is created empty, dropped, then recreated with a
	// 1h rule: the old task must not return, and only the new definition
	// may judge documents.
	require.NoError(t, coll.CreateIndex(ctx, ttlIndexInfo(time.Second)))
	require.NoError(t, coll.DropIndex(ctx, "ttl_ts"))
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "ttl_ts", Fields: []string{"ts"}, TTL: &TTLParams{ExpireAfter: time.Hour},
	}))
	var a anyenc.Arena
	// Would be expired under the old 1s rule (boundary 6_000), alive under
	// the new 1h rule (boundary ~3_610_000).
	require.NoError(t, coll.Insert(ctx, ttlDoc(&a, "d", 5_000)))
	ds.ttl.sweepOnce(ctx, []string{"c"}, now)
	_, err = coll.FindId(ctx, "d")
	require.NoError(t, err)

	// Sweeping at a time that satisfies the NEW rule does delete it.
	later := now + int64(time.Hour/time.Millisecond) - 4_000 + 1
	ds.ttl.sweepOnce(ctx, []string{"c"}, later)
	_, err = coll.FindId(ctx, "d")
	assert.ErrorIs(t, err, ErrDocNotFound)
}

func TestTTL_ConcurrentSweepsAndWrites(t *testing.T) {
	fx := newFixture(t, &Config{TTL: TTLConfig{SweepInterval: 2 * time.Millisecond}})
	ds := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, ttlIndexInfo(time.Second)))

	const n = 100
	base := time.Now().UnixMilli()
	var a anyenc.Arena
	for i := 0; i < n; i++ {
		require.NoError(t, coll.Insert(ctx, ttlDoc(&a, fmt.Sprintf("d%d", i), base-int64(i)*10)))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	// Writers keep refreshing half the documents into the future and
	// letting the other half expire.
	go func() {
		defer wg.Done()
		var ar anyenc.Arena
		for {
			select {
			case <-stop:
				return
			default:
			}
			now := time.Now().UnixMilli()
			for i := 0; i < n; i += 2 {
				ar.Reset()
				_ = coll.UpdateOne(ctx, ttlDoc(&ar, fmt.Sprintf("d%d", i), now+60_000))
			}
		}
	}()
	// Extra explicit sweeps interleaved with the background loop.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ds.ttl.sweepOnce(ctx, []string{"c"}, time.Now().UnixMilli())
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	// Stop the background loop too, so the final measurements are not
	// moved by a sweep landing between two read transactions.
	require.NoError(t, ds.ttl.stop())

	// Convergence: every remaining document is one a writer refreshed;
	// every expired document is gone. Count and Find agree, and no index
	// entry dangles (index Len equals the document count in ONE snapshot).
	final := time.Now().UnixMilli()
	ds.ttl.sweepOnce(ctx, []string{"c"}, final)

	rtx, err := coll.ReadTx(ctx)
	require.NoError(t, err)
	snapCtx := rtx.Context()
	cnt, err := coll.Count(snapCtx)
	require.NoError(t, err)
	for _, ix := range coll.GetIndexes() {
		if ix.Info().Name == "ttl_ts" {
			ni, lErr := ix.Len(snapCtx)
			require.NoError(t, lErr)
			assert.Equal(t, cnt, ni, "index entries must match surviving documents")
		}
	}
	require.NoError(t, rtx.Commit())

	remaining := 0
	for i := 0; i < n; i++ {
		if _, err := coll.FindId(ctx, fmt.Sprintf("d%d", i)); err == nil {
			remaining++
		} else {
			require.ErrorIs(t, err, ErrDocNotFound)
		}
	}
	assert.Equal(t, remaining, cnt)
}

func TestTTL_NonTTLDBHasNoMonitor(t *testing.T) {
	fx := newFixture(t)
	ds := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))
	time.Sleep(20 * time.Millisecond)
	assert.False(t, ds.ttl.isRunning())
}

func (m *ttlMonitor) isRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}
