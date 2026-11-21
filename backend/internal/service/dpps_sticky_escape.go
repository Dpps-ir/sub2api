package service

import (
	"context"
	"log/slog"
	"time"
)

// dppsStickyWaitPollInterval is how often a request waiting for its sticky seat
// retries the slot. A seat's concurrency slot is held for a whole upstream
// response, so a coarse poll costs nothing against the wait it saves.
const dppsStickyWaitPollInterval = 50 * time.Millisecond

// dppsWaitForStickySlot is the DPPS change to the "concurrency_full" sticky
// escape in selectBySessionHash. It runs right after the first slot attempt
// and returns result unchanged unless the sticky account is full only because
// its slots are taken (no error, escape enabled and allowed).
//
// Upstream then escapes at once, so a second request of a session that arrives
// while the first still streams goes to another account with a cold prompt
// cache. For a seat with concurrency 1 that is the common case (the cache warms
// only on request 3). Here the request first WAITS for the sticky slot, bounded
// by gateway.openai_scheduler.dpps_sticky_busy_wait_ms (default 3000, 0 = no wait; also never above
// gateway.scheduling.sticky_session_wait_timeout) and only while the account's
// wait queue (sticky_session_max_waiting) has room. If it gets the slot it
// returns that acquired result, and upstream's own "acquired" branch carries on
// (TTL refresh, profit gate). If the wait cannot be taken, times out or the
// request is cancelled it returns the original result and upstream escapes
// exactly as before. The TTFT and error-rate escapes are decided earlier and
// are not touched.
func (s *defaultOpenAIAccountScheduler) dppsWaitForStickySlot(
	ctx context.Context,
	req OpenAIAccountScheduleRequest,
	account *Account,
	escapeCfg openAIStickyEscapeConfig,
	result *AcquireResult,
	acquireErr error,
) *AcquireResult {
	if s == nil || s.service == nil || s.service.concurrencyService == nil || account == nil {
		return result
	}
	if !escapeCfg.enabled || req.DisableStickyEscape || acquireErr != nil || result == nil || result.Acquired {
		return result
	}
	cfg := s.service.schedulingConfig()
	// The wait is capped by gateway.openai_scheduler.dpps_sticky_busy_wait_ms
	// (default 3000, 0 = no wait) and never exceeds the general sticky wait timeout.
	waitFor := cfg.StickySessionWaitTimeout
	if s.service.cfg != nil {
		if capMs := time.Duration(s.service.cfg.Gateway.OpenAIScheduler.DppsStickyBusyWaitMs) * time.Millisecond; capMs < waitFor {
			waitFor = capMs
		}
	} else {
		waitFor = 0
	}
	if waitFor <= 0 || cfg.StickySessionMaxWaiting <= 0 {
		return result
	}
	cs := s.service.concurrencyService
	// Cheap look first: a full queue means the wait cannot be taken.
	if waiting, err := cs.GetAccountWaitingCount(ctx, account.ID); err == nil && waiting >= cfg.StickySessionMaxWaiting {
		return result
	}
	// Take a place in the queue (atomic bound), the same counter the handler's
	// own wait uses, so waiting here counts against sticky_session_max_waiting.
	canWait, err := cs.IncrementAccountWaitCount(ctx, account.ID, cfg.StickySessionMaxWaiting)
	if err != nil || !canWait {
		return result
	}
	defer cs.DecrementAccountWaitCount(ctx, account.ID)

	start := time.Now()
	timer := time.NewTimer(waitFor)
	defer timer.Stop()
	ticker := time.NewTicker(dppsStickyWaitPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return result
		case <-timer.C:
			slog.Info("dpps_sticky_wait_timeout", "account_id", account.ID, "waited", time.Since(start))
			return result
		case <-ticker.C:
		}
		got, gotErr := s.service.tryAcquireAccountSlot(ctx, account.ID, account.Concurrency)
		if gotErr != nil {
			return result
		}
		if got != nil && got.Acquired {
			slog.Info("dpps_sticky_wait_acquired", "account_id", account.ID, "waited", time.Since(start))
			return got
		}
	}
}
