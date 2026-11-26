# DPPS additions to Sub2API

This fork (`Dpps-ir/sub2api`) exists so DPPS can run a Sub2API with two DPPS
additions:

1. The **outgoing events webhook** (SC-28, D-248 to D-251): Sub2API tells one
   configured receiver (DPPS) about every change that matters (seat status and limits,
   usage, group and channel edits, price changes, balance and key events, a daily
   snapshot), signed with HMAC-SHA256 and retried from a database outbox
   (`dpps_event_outbox`). It is off until an admin enables it on the admin page
   "Events webhook" (`/admin/dpps/events-webhook`, API `/api/v1/admin/dpps/events-webhook`).
   The JSON envelope, the `X-Event-Id` and `X-Event-Signature` headers and the event
   types (`seat.status`, ...) are a contract with DPPS's receiver: do not rename them.
2. The **Seat API** (D-286): an isolated service-to-service API under `/api/v1/dpps/*`
   (`GET /seats`, `GET /seats/:id`, `PUT /seats/:id/groups`, `PUT /seats/:id/scheduling-threshold` (D-312), `POST /groups`,
   `DELETE /groups/:id`, `PUT /keys/:id/group`, `GET /usage`) guarded by its own
   `X-DPPS-Seat-Token` (`dpps_seat_<64hex>`), constant-time SHA-256 verification
   against `settings` (`dpps_seat_api_token_hash`, `dpps_seat_api_token_meta`,
   excluded from `GetAllSettings`, `UpdateSettings`, and `dppsSettingsChangedKeys`),
   pre/post-auth rate limiting (including loopback), and synchronous `audit_logs`
   recording per mutating request. Token lifecycle (`/api/v1/admin/dpps/seat-token`)
   requires human admin JWT and step-up 2FA for writes.

## The rule

- `main` is an exact copy of upstream `Wei-Shaw/sub2api`. **Never commit to it.**
  It moves only through `gh repo sync Dpps-ir/sub2api --source Wei-Shaw/sub2api`.
- All DPPS work lives on the branch `dpps` = upstream `main` + our commits, re-based
  after every sync (so `dpps` is force-pushed; use `--force-with-lease`).
- Our code lives in files that upstream does not have, named with a `dpps_` /
  `Dpps` prefix (and the new package `backend/internal/dpps`). Every Go identifier we
  add at package level carries `Dpps`/`dpps`, the table is `dpps_event_outbox`, the
  settings keys are `dpps_events_webhook_*` and `dpps_seat_api_token_*`, the lock is
  `dpps:events:webhook:sender:leader`, so a name upstream adds later cannot collide with ours.
- Inside an upstream file the only change allowed is a **hook**: ideally one line that
  calls a function in a `dpps_` file, marked with a trailing `// DPPS hook`. Put new
  logic in our files, never in the hook. A hook that has to be two lines (read before,
  report after) says why below.
- Never edit `wire_gen.go` by hand: regenerate it (see below).

## Our files (new, cannot conflict)

Backend (`backend/`):
`internal/service/dpps_events_{service,emit,types,datasource,hooks}.go` and their tests,
`internal/service/dpps_seat_{service,token_service}.go` and `dpps_seat_service_test.go`,
`internal/service/dpps_sticky_escape.go` and `dpps_q75_sticky_probe_test.go`,
`internal/service/dpps_test_hooks.go` and `dpps_test_hooks_test.go`,
`internal/repository/dpps_event_outbox_repo.go`, `dpps_account_events.go`, `dpps_seat_repo.go` and tests,
`internal/handler/dpps_gateway_served_models.go`, `dpps_seat_{handler,dto}.go` and test,
`internal/handler/admin/dpps_{events_webhook,seat_token}_handler.go`,
`internal/server/middleware/dpps_seat_{auth,ratelimit,audit}.go`,
`internal/server/routes/dpps_admin.go`, `dpps_seats.go`, `dpps_admin_test.go`, `dpps_seat_api_test.go`,
`internal/dpps/wire.go`,
`migrations/253_dpps_event_outbox.sql`.

Frontend (`frontend/src/`):
`api/admin/dppsEventsWebhook.ts`, `views/admin/DppsEventsWebhookView.vue`,
`views/admin/dppsEventsWebhookForm.ts`, `router/dppsRoutes.ts`,
`i18n/locales/{en,zh}/admin/dppsEventsWebhook.ts` (also holds the sidebar label,
`admin.dppsEventsWebhook.nav`), and the specs `views/admin/__tests__/DppsEventsWebhookView.spec.ts`
and `api/__tests__/dppsEventsWebhook.spec.ts`.

Also `DPPS.md` and `tools/dpps-sync.sh` (the root `scripts/` folder is in upstream's
`.gitignore`, so the script lives in `tools/`).

## Hook sites (every `// DPPS hook` line)

Find them all with `git grep -n -E '(// DPPS hook|<!-- DPPS hook -->)$' -- backend frontend`
(the sync script counts them before and after a rebase and fails if the number drops).
`go generate` needs nothing here: wire picks the DPPS providers up from one set.

Backend, wiring:

| file | lines | what it calls |
|---|---|---|
| `cmd/server/wire.go` | 4 | import of `internal/dpps`; `dpps.ProviderSet` in `wire.Build` (the outbox repository, events service, and `ProvideDppsSeatRuntime` for the Seat API services and middleware repositories); a `dppsEvents *service.DppsEventsService` parameter of `provideCleanup`; one cleanup step `dppsEvents.Stop()`. The parameter is also what makes wire build the DPPS services. |
| `cmd/server/wire_gen_test.go` | 1 | passes `&service.DppsEventsService{}` for that parameter |
| `cmd/server/wire_gen.go` | generated | regenerate, never merge by hand |
| `internal/server/router.go` | 1 | `routes.RegisterDppsSeatRoutes(v1, redisClient)` (mounts `/api/v1/dpps/*` directly on `v1`, isolated from `AdminAuth`, `JWTAuth`, `APIKeyAuth`, `PanelRateLimiter`, and `AdminComplianceGuard`) |
| `internal/server/routes/admin.go` | 1 | `registerDppsAdminRoutes(admin, stepUpAuth)` (our `routes/dpps_admin.go`, registering `/dpps/events-webhook` and `/dpps/seat-token`; always registers: the router is built before DPPS services in the wire graph, so handlers resolve services per request and answer 503 until initialized) |

Backend, accounts (`internal/repository/account_repo.go`, 18 lines):

| function | calls |
|---|---|
| `SetError` | 2 lines: `r.dppsSeatInError` before the write, `service.DppsEmitSeatError` after (seat.status is a transition, so the old status must be read first) |
| `SetGrokCredentialErrorIfMatch`, `SetGrokOAuthErrorIfCredentialsUnchanged` | 1 line each: `service.DppsEmitSeatError` |
| `ClearError` | 2 lines: `r.dppsSeatInError`, `service.DppsEmitSeatRecovered` |
| `SetRateLimited`, `SetRateLimitedIfLater`, `SetRateLimitedIfUnchanged`, `SetOverloaded`, `SetTempUnschedulable`, `SetGrokCredentialTempUnschedulableIfMatch` | 1 line each: `service.DppsEmitSeatLimited` |
| `ClearTempUnschedulable`, `ClearRateLimit` | 2 lines each: `r.dppsHoldsTempPause` / `r.dppsHoldsRateLimit` before, `service.DppsEmitSeatClearedIf` after (callers clear defensively, so only a seat that held a limit counts as cleared) |
| `SetSchedulable` | 1 line: `service.DppsEmitSeatSchedulable` |
| `AutoPauseExpiredAccounts` | 1 line: `service.DppsEmitSeatsAutoPaused` |

Backend, other:

| file | lines | what it calls |
|---|---|---|
| `internal/repository/usage_billing_repo.go` | 1 | `service.DppsAfterBillingApply` (after the commit) |
| `internal/service/admin_account.go` | 6 | `dppsAfterAccountCreate`, `dppsAfterAccountUpdate`; bulk edit: `dppsEventChangedFields(input)` first (the body rewrites the input) and `dppsAfterAccountBulkUpdate` before the success return; delete: `dppsBeforeAccountDelete` (reads the name while the row exists) and `dppsDone()` |
| `internal/service/admin_group.go` | 4 | `dppsAfterGroupCreate`, `dppsAfterGroupUpdate`; `deleteGroup`: `dppsBeforeGroupDelete` and `dppsDone()` |
| `internal/service/channel_service.go` | 4 | `dppsAfterChannelCreate`, `dppsAfterChannelUpdate`; `Delete`: `dppsBeforeChannelDelete` and `dppsDone()` |
| `internal/service/admin_user.go` | 1 | `DppsEmitBalanceChangedIfDiff` |
| `internal/service/api_key_service.go` | 1 | `DppsEmitKeyUpdated` |
| `internal/service/openai_gateway_usage.go` | 1 | `DppsEmitSeatUsageFromExtra` |
| `internal/service/ops_alert_evaluator_service.go` | 2 | `DppsEmitAlertFromEvent` (fired, resolved) |
| `internal/service/pricing_service.go` | 4 | `dppsOldPricing := s.pricingData` and `s.dppsPricesCustomReloaded(...)` in `reloadCustomPricingLayers`; the same pair with `s.dppsPricesLoaded(...)` in `loadPricingData` |
| `internal/service/openai_account_scheduler.go` | 1 | `s.dppsWaitForStickySlot(...)` in `selectBySessionHash`, right after the first sticky slot attempt (our `dpps_sticky_escape.go`): when the sticky account is full only because its slots are taken (the `concurrency_full` escape), the request first waits for the slot, bounded by `min(gateway.openai_scheduler.dpps_sticky_busy_wait_ms, gateway.scheduling.sticky_session_wait_timeout)` and only while the account's wait queue (`sticky_session_max_waiting`) has room, and returns the acquired slot into upstream's own acquired branch; on no room, timeout or cancel it returns the result unchanged and upstream escapes as before. TTFT and error-rate escapes are untouched. One line. |
| `internal/config/config.go` | 2 | the field `DppsStickyBusyWaitMs` (`gateway.openai_scheduler.dpps_sticky_busy_wait_ms`, ms) in `GatewayOpenAISchedulerConfig` and its `viper.SetDefault(..., 3000)`. Default 3000: a request whose sticky seat is busy waits at most 3 s for it, then escapes as upstream does. `0` (or a negative value) means no wait, the upstream behaviour. Never above `sticky_session_wait_timeout`. Env: `GATEWAY_OPENAI_SCHEDULER_DPPS_STICKY_BUSY_WAIT_MS`. |
| `internal/service/setting_update.go` | 2 (changed, not added) | `s.dppsSetMultiple(ctx, updates)` replaces `s.settingRepo.SetMultiple(ctx, updates)` in the two update paths |
| `internal/service/openai_gateway_forward.go` | 4 | `dppsPromoteDeveloperPrompt(decoded, s.cfg, isCodexCLI)` (1 line in the non-bridge OAuth branch just before `applyCodexOAuthTransformWithOptions`: it must run before `extractSystemMessagesFromInput`/`applyInstructions` which live inside the transform and have no config access, and only there because the Chat-compat bridge sets `SkipDefaultInstructions`); plus 3 test-hook lines in `buildUpstreamRequest`: `dppsResolveChatGPTURL` (OAuth, SetupToken OAuth-like) and `dppsApplyChatGPTHost` |
| `internal/service/openai_gateway_passthrough.go` | 3 | `dppsResolveChatGPTURL` (OAuth, SetupToken OAuth-like) and `dppsApplyChatGPTHost` in `buildUpstreamRequestOpenAIPassthrough` |
| `internal/service/openai_ws_forwarder_payload.go` | 2 | `dppsResolveChatGPTURL` (OAuth, SetupToken OAuth-like) in `buildOpenAIResponsesWSURL` |
| `internal/service/account_usage_service.go` | 3 | `DppsEmitSeatUsageFromExtra` (1 line); `dppsResolveChatGPTURL` and `dppsApplyChatGPTHost` in `probeOpenAICodexSnapshot` (2 lines) |
| `internal/service/account_test_service.go` | 6 | `dppsResolveChatGPTURL` and `dppsApplyChatGPTHost` in `testOpenAIAccountConnection`, `testOpenAICompactConnection`, and `testOpenAIImageOAuth` |
| `internal/service/openai_quota_service.go` | 3 | `dppsResolveChatGPTURL` on `chatGPTUsageURL`, `chatGPTRateLimitCreditsURL`, and `chatGPTRateLimitResetURL` |
| `internal/service/openai_codex_models_service.go` | 1 | `dppsResolveChatGPTURL(chatgptCodexModelsURL)` in `FetchCodexModelsManifest` |
| `internal/config/config.go` | 2 | the field `Gateway.DppsDeveloperPromptAsInstructions` and its `viper.SetDefault(..., false)` |

Settings (config, not the admin page): `gateway.dpps_developer_prompt_as_instructions` (bool,
default **true**; `false` (yaml, or env `GATEWAY_DPPS_DEVELOPER_PROMPT_AS_INSTRUCTIONS=false`) restores the old behaviour). When true, an OpenAI OAuth `/v1/responses` request that has no `instructions`, is not
from the Codex CLI (`IsCodexOfficialClientByHeaders` or `force_codex_cli`) and carries
`role:"developer"` input items has those items treated like `role:"system"` ones: their text becomes
`instructions` (in order) so the Codex persona is not injected. Logic and tests:
`internal/service/dpps_developer_prompt{,_test}.go`. Default on because it was proven live (2026-11, GPT-6 Luna through
production, 5+5 requests with an ask_user_question tool and an ambiguous request: client prompt as
`developer` with the persona injected called ask_user_question 0 of 5 times, as `system` 5 of 5).

Test-only env hooks (never set in production; exact no-ops when unset):
- `SUB2API_TEST_CHATGPT_BASE_URL`: accepts only `http`/`https` with no userinfo/query/fragment and strict loopback host (`localhost` or `net.ParseIP(host).IsLoopback()`); rewrites the hardcoded ChatGPT upstream endpoints and `req.Host` in `internal/service/dpps_test_hooks.go`.
- `SUB2API_TEST_DPPS_ALLOW_LOOPBACK_WEBHOOK=true`: inside `internal/service/dpps_events_service.go`, allows `http`/`https` webhook URLs only when the host is strictly loopback and switches `DppsEventsService`'s dialer from `safeDialContext` to a loopback-only `DialContext`. Non-loopback HTTP and private LAN IPs remain rejected.

Frontend:

| file | lines | what it calls |
|---|---|---|
| `src/router/index.ts` | 2 | `import { dppsRoutes }` and `...dppsRoutes,` first in `routes` (before any catch-all) |
| `src/components/layout/AppSidebar.vue` | 1 | the sidebar entry `/admin/dpps/events-webhook` |
| `src/i18n/locales/{en,zh}/admin/index.ts` | 2 each | import and spread of `dppsEventsWebhook` |

## Sync procedure

Run `tools/dpps-sync.sh` from a clone of the fork with `dpps` checked out and a clean tree.
It never pushes. It does:

1. refuses a dirty tree or any branch but `dpps`;
2. `gh repo sync Dpps-ir/sub2api --source Wei-Shaw/sub2api` (without `gh`: fetches
   `upstream` and fast-forwards only the local `main`, refusing if `main` has commits of
   its own; the fork on GitHub then stays unsynced);
3. `git fetch origin`, then `git rebase origin/main` onto `dpps`;
4. on a conflict stops with the conflicted files. Every conflict is on a hook line or next
   to one: keep upstream's text and re-add the `// DPPS hook` line. For `wire_gen.go` take
   either side and regenerate it:
   `cd backend && GOFLAGS=-mod=mod go run github.com/google/wire/cmd/wire ./cmd/server`
   (same as `go generate ./cmd/server` when `go.sum` already lists `google/subcommands`),
   then `git checkout go.mod go.sum` if wire touched them; then `git add` and `git rebase --continue`;
5. checks that the number of hook lines did not drop, runs `go build ./...`, `go vet` on our
   packages, our unit tests (`-tags unit -run 'Dpps|TestProvideCleanup...'`, which includes the
   wire check) and, when `frontend/node_modules` exists, `vue-tsc --noEmit` and our vitest specs
   plus the i18n and router specs;
6. prints `git push --force-with-lease origin dpps` for you to run.

Not run by the script: the repository integration tests (`go test -tags integration
./internal/repository/ -run Dpps`, they need Docker testcontainers).

Migrations: Sub2API applies every `migrations/*.sql` it has not applied yet, keyed by the
full file name, so `253_dpps_event_outbox.sql` cannot clash with an upstream `253_*.sql`.
Never edit that file after it was applied somewhere (the runner stores its checksum); add a
new `dpps_` migration instead.
