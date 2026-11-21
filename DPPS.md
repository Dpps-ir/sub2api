# DPPS additions to Sub2API

This fork (`Dpps-ir/sub2api`) exists so DPPS can run a Sub2API with one extra
feature, the **outgoing events webhook** (SC-28, D-248 to D-251): Sub2API tells one
configured receiver (DPPS) about every change that matters (seat status and limits,
usage, group and channel edits, price changes, balance and key events, a daily
snapshot), signed with HMAC-SHA256 and retried from a database outbox
(`dpps_event_outbox`). It is off until an admin enables it on the admin page
"Events webhook" (`/admin/dpps/events-webhook`, API `/api/v1/admin/dpps/events-webhook`).
The JSON envelope, the `X-Event-Id` and `X-Event-Signature` headers and the event
types (`seat.status`, ...) are a contract with DPPS's receiver: do not rename them.

## The rule

- `main` is an exact copy of upstream `Wei-Shaw/sub2api`. **Never commit to it.**
  It moves only through `gh repo sync Dpps-ir/sub2api --source Wei-Shaw/sub2api`.
- All DPPS work lives on the branch `dpps` = upstream `main` + our commits, re-based
  after every sync (so `dpps` is force-pushed; use `--force-with-lease`).
- Our code lives in files that upstream does not have, named with a `dpps_` /
  `Dpps` prefix (and the new package `backend/internal/dpps`). Every Go identifier we
  add at package level carries `Dpps`/`dpps`, the table is `dpps_event_outbox`, the
  settings keys are `dpps_events_webhook_*`, the lock is `dpps:events:webhook:sender:leader`,
  so a name upstream adds later cannot collide with ours.
- Inside an upstream file the only change allowed is a **hook**: ideally one line that
  calls a function in a `dpps_` file, marked with a trailing `// DPPS hook`. Put new
  logic in our files, never in the hook. A hook that has to be two lines (read before,
  report after) says why below.
- Never edit `wire_gen.go` by hand: regenerate it (see below).

## Our files (new, cannot conflict)

Backend (`backend/`):
`internal/service/dpps_events_{service,emit,types,datasource,hooks}.go` and their tests,
`internal/repository/dpps_event_outbox_repo.go`, `dpps_account_events.go` and tests,
`internal/handler/dpps_gateway_served_models.go` and test,
`internal/handler/admin/dpps_events_webhook_handler.go`,
`internal/server/routes/dpps_admin.go` and test, `internal/dpps/wire.go`,
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
| `cmd/server/wire.go` | 4 | import of `internal/dpps`; `dpps.ProviderSet` in `wire.Build` (the outbox repository and the events service, which needs `*handler.GatewayHandler` so it can register the served-models lister); a `dppsEvents *service.DppsEventsService` parameter of `provideCleanup`; one cleanup step `dppsEvents.Stop()`. The parameter is also what makes wire build the service. |
| `cmd/server/wire_gen_test.go` | 1 | passes `&service.DppsEventsService{}` for that parameter |
| `cmd/server/wire_gen.go` | generated | regenerate, never merge by hand |
| `internal/server/routes/admin.go` | 1 | `registerDppsAdminRoutes(admin, stepUpAuth)` (our `routes/dpps_admin.go`; always registers: the router is built before the events service in the wire graph, so the handler looks the service up per request and answers 503 until it exists) |

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
| `internal/service/account_usage_service.go`, `openai_gateway_usage.go` | 1 each | `DppsEmitSeatUsageFromExtra` |
| `internal/service/ops_alert_evaluator_service.go` | 2 | `DppsEmitAlertFromEvent` (fired, resolved) |
| `internal/service/pricing_service.go` | 4 | `dppsOldPricing := s.pricingData` and `s.dppsPricesCustomReloaded(...)` in `reloadCustomPricingLayers`; the same pair with `s.dppsPricesLoaded(...)` in `loadPricingData` |
| `internal/service/setting_update.go` | 2 (changed, not added) | `s.dppsSetMultiple(ctx, updates)` replaces `s.settingRepo.SetMultiple(ctx, updates)` in the two update paths |

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
