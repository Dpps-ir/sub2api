#!/usr/bin/env bash
# DPPS sync: bring the fork's main up to upstream, rebase the `dpps` branch onto it,
# run our checks, and print the push command. It never pushes. See DPPS.md.
#
# Run it from a clone of the fork with the `dpps` branch checked out and a clean tree.
#
# Environment (defaults in brackets; the test setup overrides them):
#   DPPS_BRANCH [dpps]            the branch that carries our changes
#   DPPS_MAIN [main]              the branch that mirrors upstream (never committed to)
#   DPPS_ORIGIN [origin]          remote of the fork
#   DPPS_UPSTREAM [upstream]      remote of upstream (used only when gh is missing)
#   DPPS_FORK_REPO [Dpps-ir/sub2api]   owner/name of the fork, for gh
#   DPPS_UPSTREAM_REPO [Wei-Shaw/sub2api]    owner/name of upstream, for gh
#   DPPS_GH [gh]                  the gh command; set it to a missing command to use the fallback
#   DPPS_SKIP_CHECKS [unset]      set to 1 to stop after the rebase (no build, test or frontend check)
set -euo pipefail

BRANCH="${DPPS_BRANCH:-dpps}"
MAIN="${DPPS_MAIN:-main}"
ORIGIN="${DPPS_ORIGIN:-origin}"
UPSTREAM="${DPPS_UPSTREAM:-upstream}"
FORK_REPO="${DPPS_FORK_REPO:-Dpps-ir/sub2api}"
UPSTREAM_REPO="${DPPS_UPSTREAM_REPO:-Wei-Shaw/sub2api}"
GH="${DPPS_GH:-gh}"

die() { echo "dpps-sync: $*" >&2; exit 1; }
step() { echo; echo "== $*"; }

ROOT="$(git rev-parse --show-toplevel)" || die "not inside a git repository"
cd "$ROOT"

# The hook marker every one-line edit inside an upstream file carries (see DPPS.md).
HOOK_RE='(// DPPS hook|<!-- DPPS hook -->)$'
hook_lines() { git grep -n -E "$HOOK_RE" -- backend frontend | sort; }

# ---- 1. preconditions ------------------------------------------------------------
step "1. checking the tree"
[ "$(git rev-parse --abbrev-ref HEAD)" = "$BRANCH" ] || die "check out '$BRANCH' first (on '$(git rev-parse --abbrev-ref HEAD)')"
if [ -n "$(git status --porcelain)" ]; then
  git status --short
  die "the working tree is not clean: commit or move your changes first"
fi
ORIG_HEAD_SHA="$(git rev-parse HEAD)"
HOOKS_BEFORE="$(hook_lines | wc -l | tr -d ' ')"
[ "$HOOKS_BEFORE" -gt 0 ] || die "found no '// DPPS hook' lines on $BRANCH: wrong branch?"
echo "on $BRANCH at $ORIG_HEAD_SHA with $HOOKS_BEFORE hook lines"

# ---- 2. move main -----------------------------------------------------------------
if command -v "$GH" >/dev/null 2>&1; then
  step "2. gh repo sync $FORK_REPO --source $UPSTREAM_REPO"
  "$GH" repo sync "$FORK_REPO" --source "$UPSTREAM_REPO"
  step "3. fetching $ORIGIN"
  git fetch "$ORIGIN"
  BASE="$ORIGIN/$MAIN"
else
  step "2. gh is missing: fast-forwarding local $MAIN from $UPSTREAM"
  git fetch "$UPSTREAM"
  git rev-parse --verify -q "refs/heads/$MAIN" >/dev/null || die "no local '$MAIN' branch"
  if ! git merge-base --is-ancestor "$MAIN" "$UPSTREAM/$MAIN"; then
    die "local '$MAIN' has commits of its own (or diverged from $UPSTREAM/$MAIN); '$MAIN' must stay an exact copy of upstream"
  fi
  # Fast-forward the ref without checking it out (we are on $BRANCH).
  git update-ref "refs/heads/$MAIN" "$UPSTREAM/$MAIN" "$(git rev-parse "refs/heads/$MAIN")"
  echo "local $MAIN is now at $(git rev-parse --short "$MAIN"); the fork on GitHub is NOT synced (install gh, or push $MAIN yourself)"
  BASE="$MAIN"
fi

# ---- 3. rebase --------------------------------------------------------------------
step "4. rebasing $BRANCH onto $BASE ($(git rev-parse --short "$BASE"))"
if ! git rebase "$BASE"; then
  echo
  CONFLICTS="$(git diff --name-only --diff-filter=U)"
  if [ -z "$CONFLICTS" ]; then
    echo "dpps-sync: the rebase failed without a conflict (see the git message above, for example a missing committer identity)." >&2
    echo "Fix that and run: git rebase --continue   (or give up with: git rebase --abort)" >&2
    exit 2
  fi
  echo "dpps-sync: REBASE STOPPED on a conflict. Conflicted files:" >&2
  echo "$CONFLICTS" | sed 's/^/  /' >&2
  if echo "$CONFLICTS" | grep -q 'wire_gen\.go$'; then
    echo "hint: regenerate wire_gen.go with go generate ./cmd/server (do not merge it by hand):" >&2
    echo "      take either side of backend/cmd/server/wire_gen.go, finish the other files, then" >&2
    echo "      cd backend && GOFLAGS=-mod=mod go run github.com/google/wire/cmd/wire ./cmd/server" >&2
    echo "      and git checkout backend/go.sum backend/go.mod if wire touched them" >&2
  fi
  echo "Every hook line is marked '// DPPS hook' (DPPS.md lists them). When done: git add <files> && git rebase --continue" >&2
  echo "To give up: git rebase --abort (your branch stays at $ORIG_HEAD_SHA)" >&2
  exit 2
fi

HOOKS_AFTER="$(hook_lines | wc -l | tr -d ' ')"
if [ "$HOOKS_AFTER" -ne "$HOOKS_BEFORE" ]; then
  echo "dpps-sync: the number of '// DPPS hook' lines changed: $HOOKS_BEFORE before, $HOOKS_AFTER after the rebase." >&2
  echo "A hook may have been dropped (or the code around it moved). Compare with: git diff $ORIG_HEAD_SHA HEAD" >&2
  hook_lines >&2
  exit 3
fi
echo "rebase done: $HOOKS_AFTER hook lines kept"

if [ "${DPPS_SKIP_CHECKS:-}" = "1" ]; then
  echo "DPPS_SKIP_CHECKS=1: skipping the checks"
else
  # ---- 4. backend checks ----------------------------------------------------------
  step "5. backend: go build ./..."
  (cd backend && go build ./...)

  VET_PKGS=(./cmd/server/ ./internal/dpps/ ./internal/service/ ./internal/repository/ ./internal/handler/ ./internal/handler/admin/ ./internal/server/routes/)
  step "6. backend: go vet on our packages"
  (cd backend && go vet -tags unit "${VET_PKGS[@]}")

  step "7. backend: our unit tests (and the wire check)"
  OUT="$(mktemp)"
  trap 'rm -f "$OUT"' EXIT
  if ! (cd backend && go test -tags unit -count=1 -v \
        -run 'Dpps|TestProvideCleanup_WithMinimalDependencies_NoPanic' \
        ./cmd/server/ ./internal/dpps/ ./internal/service/ ./internal/repository/ ./internal/handler/ ./internal/handler/admin/ ./internal/server/routes/ >"$OUT" 2>&1); then
    tail -60 "$OUT" >&2
    die "our backend tests failed (full output above)"
  fi
  PASSED="$(grep -c -E '^--- PASS: ' "$OUT" || true)"
  # "Tests: 0 total" is a failure: a rename that stops '-run Dpps' matching must not pass silently.
  [ "$PASSED" -ge 30 ] || die "only $PASSED top-level tests ran (expected 30 or more): did a rename break the -run pattern?"
  grep -q -E '^--- PASS: TestProvideCleanup_WithMinimalDependencies_NoPanic' "$OUT" || die "the wire check test did not run"
  echo "backend: $PASSED top-level tests passed"
  echo "not run here: the repository integration tests (build tag integration; they need Docker testcontainers):"
  echo "  cd backend && go test -tags integration ./internal/repository/ -run Dpps"

  # ---- 5. frontend checks ---------------------------------------------------------
  step "8. frontend: vue-tsc and our specs"
  if [ -x frontend/node_modules/.bin/vue-tsc ]; then
    (
      cd frontend
      node_modules/.bin/vue-tsc --noEmit
      node_modules/.bin/vitest run \
        src/views/admin/__tests__/DppsEventsWebhookView.spec.ts \
        src/api/__tests__/dppsEventsWebhook.spec.ts \
        src/i18n src/router src/components/layout
    )
  else
    echo "frontend/node_modules is missing: skipped. Run 'cd frontend && pnpm install --frozen-lockfile' and re-run (never commit a lockfile change)."
  fi
fi

step "done"
echo "$BRANCH is rebased onto $BASE. Old tip: $ORIG_HEAD_SHA (git reset --hard $ORIG_HEAD_SHA undoes it)."
echo
echo "Publish it yourself (this script never pushes):"
echo "  git push --force-with-lease $ORIGIN $BRANCH"
