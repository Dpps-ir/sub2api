package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// q75: why can request 2 of one session land on another account?
// slotTrackingConcurrencyCache is a real counting slot cache (the existing
// schedulerTestConcurrencyCache answers from a static map and cannot show a
// slot held by a still-running request 1).
type slotTrackingConcurrencyCache struct {
	ConcurrencyCache
	mu      sync.Mutex
	slots   map[int64]map[string]bool
	waiting map[int64]int
}

func (c *slotTrackingConcurrencyCache) IncrementAccountWaitCount(_ context.Context, id int64, maxWait int) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waiting == nil {
		c.waiting = map[int64]int{}
	}
	if c.waiting[id] >= maxWait {
		return false, nil
	}
	c.waiting[id]++
	return true, nil
}

func (c *slotTrackingConcurrencyCache) DecrementAccountWaitCount(_ context.Context, id int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waiting[id] > 0 {
		c.waiting[id]--
	}
	return nil
}

func (c *slotTrackingConcurrencyCache) waitingNow(id int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiting[id]
}

func (c *slotTrackingConcurrencyCache) AcquireAccountSlot(_ context.Context, id int64, max int, reqID string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.slots == nil {
		c.slots = map[int64]map[string]bool{}
	}
	if len(c.slots[id]) >= max {
		return false, nil
	}
	if c.slots[id] == nil {
		c.slots[id] = map[string]bool{}
	}
	c.slots[id][reqID] = true
	return true, nil
}

func (c *slotTrackingConcurrencyCache) ReleaseAccountSlot(_ context.Context, id int64, reqID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.slots[id], reqID)
	return nil
}

func (c *slotTrackingConcurrencyCache) GetAccountsLoadBatch(_ context.Context, accounts []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int64]*AccountLoadInfo{}
	for _, a := range accounts {
		rate := 0
		if a.MaxConcurrency > 0 {
			rate = len(c.slots[a.ID]) * 100 / a.MaxConcurrency
		}
		out[a.ID] = &AccountLoadInfo{AccountID: a.ID, LoadRate: rate}
	}
	return out, nil
}

func (c *slotTrackingConcurrencyCache) GetAccountWaitingCount(_ context.Context, id int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiting[id], nil
}

type q75Opts struct {
	conc            int
	req1HeldAtReq2  bool
	waitTimeout     time.Duration // DppsStickyBusyWaitMs (general timeout fixed at 45s)
	maxWaiting      int           // StickySessionMaxWaiting
	freeReq1After   time.Duration // >0: request 1 finishes this long after request 2 starts
	preloadedWaiter int           // requests already queued on account 1
}

type q75Result struct {
	ids     [3]int64
	layers  [3]string
	acq     [3]bool
	waitPln [3]bool
	binding int64
	elapsed time.Duration // time request 2 took to be scheduled
	cache   *slotTrackingConcurrencyCache
}

func q75Run(t *testing.T, o q75Opts) q75Result {
	ctx := context.Background()
	groupID := int64(7500 + int64(o.conc))
	accounts := []Account{
		{ID: 75001, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: o.conc, Priority: 0, GroupIDs: []int64{groupID}},
		{ID: 75002, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: o.conc, Priority: 0, GroupIDs: []int64{groupID}},
	}
	cache := &schedulerTestGatewayCache{}
	slots := &slotTrackingConcurrencyCache{}
	if o.preloadedWaiter > 0 {
		slots.waiting = map[int64]int{75001: o.preloadedWaiter}
	}
	cfg := &config.Config{} // production-like defaults: sticky escape on
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 45 * time.Second
	cfg.Gateway.OpenAIScheduler.DppsStickyBusyWaitMs = int(o.waitTimeout / time.Millisecond)
	cfg.Gateway.Scheduling.StickySessionMaxWaiting = o.maxWaiting
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cache:              cache,
		cfg:                cfg,
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
		concurrencyService: NewConcurrencyService(slots),
		openaiAccountStats: newOpenAIAccountRuntimeStats(),
	}
	var res q75Result
	res.cache = slots
	var releases []func()
	for i := 0; i < 3; i++ {
		// a request from request 3 onward finds request 1 finished
		if i == 2 || (i == 1 && !o.req1HeldAtReq2) {
			for _, r := range releases {
				r()
			}
			releases = nil
		}
		if i == 1 && o.req1HeldAtReq2 && o.freeReq1After > 0 {
			held := releases
			releases = nil
			time.AfterFunc(o.freeReq1After, func() {
				for _, r := range held {
					r()
				}
			})
		}
		start := time.Now()
		sel, dec, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "sess-q75", "gpt-6.2-sol", nil, OpenAIUpstreamTransportAny, false)
		if i == 1 {
			res.elapsed = time.Since(start)
		}
		require.NoError(t, err)
		require.NotNil(t, sel)
		require.NotNil(t, sel.Account)
		res.ids[i] = sel.Account.ID
		res.layers[i] = dec.Layer
		res.acq[i] = sel.Acquired
		res.waitPln[i] = sel.WaitPlan != nil
		if sel.ReleaseFunc != nil {
			releases = append(releases, sel.ReleaseFunc)
		}
	}
	for _, r := range releases {
		r()
	}
	res.binding = cache.sessionBindings["openai:sess-q75"]
	return res
}

// q75 fix: request 2 arrives while request 1 still streams and the sticky
// account has no free slot. It now WAITS for the slot (bounded by the sticky
// wait timeout) and stays on the sticky account, so its prompt cache is warm.
func TestDppsQ75_SlowFirstRequest_Concurrency1_Request2WaitsForStickySeat(t *testing.T) {
	r := q75Run(t, q75Opts{conc: 1, req1HeldAtReq2: true, waitTimeout: 3 * time.Second, maxWaiting: 3, freeReq1After: 150 * time.Millisecond})
	t.Logf("accounts=%v layers=%v acquired=%v elapsed=%s", r.ids, r.layers, r.acq, r.elapsed)
	require.Equal(t, r.ids[0], r.binding)
	require.Equal(t, r.ids[0], r.ids[1], "request 2 waited for and kept the sticky account")
	require.Equal(t, openAIAccountScheduleLayerSessionSticky, r.layers[1])
	require.True(t, r.acq[1], "request 2 holds the slot, no wait plan left for the handler")
	require.GreaterOrEqual(t, r.elapsed, 100*time.Millisecond, "it really waited")
	require.Less(t, r.elapsed, 2*time.Second)
	require.Equal(t, r.ids[0], r.ids[2])
	require.Zero(t, r.cache.waitingNow(r.ids[0]), "wait queue entry released")
}

// The wait is bounded: when the slot never frees within the wait timeout the
// request escapes as before (binding stays on the sticky account).
func TestDppsQ75_SlowFirstRequest_Concurrency1_EscapesWhenSlotNeverFrees(t *testing.T) {
	r := q75Run(t, q75Opts{conc: 1, req1HeldAtReq2: true, waitTimeout: 300 * time.Millisecond, maxWaiting: 3})
	t.Logf("accounts=%v layers=%v elapsed=%s", r.ids, r.layers, r.elapsed)
	require.Equal(t, r.ids[0], r.binding)
	require.NotEqual(t, r.ids[0], r.ids[1], "request 2 escaped after the wait timed out")
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, r.layers[1])
	require.GreaterOrEqual(t, r.elapsed, 250*time.Millisecond, "it waited the whole timeout first")
	require.Less(t, r.elapsed, 2*time.Second)
	require.Equal(t, r.ids[0], r.ids[2])
	require.Zero(t, r.cache.waitingNow(r.ids[0]), "wait queue entry released")
}

// A full wait queue means the wait cannot be taken: escape at once, no waiting.
func TestDppsQ75_SlowFirstRequest_Concurrency1_EscapesAtOnceWhenQueueFull(t *testing.T) {
	r := q75Run(t, q75Opts{conc: 1, req1HeldAtReq2: true, waitTimeout: 3 * time.Second, maxWaiting: 2, preloadedWaiter: 2})
	t.Logf("accounts=%v layers=%v elapsed=%s", r.ids, r.layers, r.elapsed)
	require.NotEqual(t, r.ids[0], r.ids[1])
	require.Less(t, r.elapsed, 500*time.Millisecond, "queue full: no waiting")
}

// No wait configured (zero timeout or zero queue) keeps the old immediate escape.
func TestDppsQ75_SlowFirstRequest_Concurrency1_ZeroBusyWaitEscapesAtOnce(t *testing.T) {
	r := q75Run(t, q75Opts{conc: 1, req1HeldAtReq2: true})
	require.NotEqual(t, r.ids[0], r.ids[1])
	require.Less(t, r.elapsed, 500*time.Millisecond)
}

// Control (Luna-like): request 1 already finished -> every request on one account.
func TestDppsQ75_FastFirstRequest_Concurrency1_AllSameAccount(t *testing.T) {
	r := q75Run(t, q75Opts{conc: 1, waitTimeout: 3 * time.Second, maxWaiting: 3})
	require.Equal(t, r.ids[0], r.ids[1])
	require.Equal(t, r.ids[0], r.ids[2])
}

// Control: free slot on the sticky account (concurrency 3) -> stays sticky even
// with request 1 still running, with no waiting.
func TestDppsQ75_SlowFirstRequest_Concurrency3_StaysSticky(t *testing.T) {
	r := q75Run(t, q75Opts{conc: 3, req1HeldAtReq2: true, waitTimeout: 3 * time.Second, maxWaiting: 3})
	require.Equal(t, r.ids[0], r.ids[1])
	require.Equal(t, r.ids[0], r.ids[2])
	require.Less(t, r.elapsed, 500*time.Millisecond)
}

// The DPPS cap wins over a longer general sticky wait timeout (45s in q75Run).
func TestDppsQ75_BusyWaitCappedBelowGeneralStickyTimeout(t *testing.T) {
	r := q75Run(t, q75Opts{conc: 1, req1HeldAtReq2: true, waitTimeout: 200 * time.Millisecond, maxWaiting: 3})
	require.NotEqual(t, r.ids[0], r.ids[1])
	require.GreaterOrEqual(t, r.elapsed, 150*time.Millisecond)
	require.Less(t, r.elapsed, 2*time.Second)
}
