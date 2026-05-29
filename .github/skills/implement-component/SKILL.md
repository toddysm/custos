---
name: implement-component
description: Implement an already-designed component end-to-end. Reads design/components/<comp>/ for the source of truth, derives a phased task plan (gate 1), files GitHub task issues + a tracker (gate 2), then autonomously implements each task — branch from main, code + tests, push, PR, wait for Copilot review (default 10 min), address review comments + CI failures, merge, tick tracker, move to next task. Auto-closes the tracker when every task is done. Triggered by phrases like "implement component <name>", "continue implementation", "plan tasks for <component>", or directly via /implement-component.
user-invocable: true
metadata:
  version: 1.0.0
---

# Implement Component Skill

You drive an already-designed component from `design/components/<comp>/` through GitHub-tracked phased implementation. The lifecycle has **three user gates** — `plan` → `file` → `execute` — followed by autonomous task-by-task execution until the tracker is fully ticked and auto-closed.

The repository's design documents under `design/` are the authoritative source of truth. You do **not** propose component designs; you turn the existing design into a plan, file it as issues, and execute it.

---

## Invocation

The user invokes this skill via `/implement-component [subcommand]`:

| Subcommand | Behavior |
|---|---|
| _(none)_ | Auto-detect: if a tracker is open and unfinished, resume execution; otherwise ask which component to plan |
| `plan <component>` | Read `design/components/<component>/` and derive a phased task plan; **wait for approval** |
| `file <component>` | Create GitHub task issues + a tracker from the approved plan |
| `execute <tracker#>` | Run the autonomous execution loop against an existing tracker |
| `resume` | Resume an in-flight branch or PR |
| `status` | Print tracker progress + current branch + open PR + CI + review state |

---

## Hard rules (Invariants)

These rules are non-negotiable. Re-read them at the start of every session.

- **Three user gates** before autonomous work begins: `plan` → `file` → `execute`. Each gate is an explicit stop-and-wait point.
- Plans are derived **fresh** from `design/components/<comp>/`. Never seed tasks from existing `todos.md` entries — those are stale or partial.
- At the start of Phase 3 (Execute), **ask the user for the Copilot review wait window**. If unanswered within a short prompt, default to **10 minutes**.
- `PAGER=cat` on **every** `gh` command.
- **Never** inline a multi-line or backtick-bearing body into a double-quoted `gh` argument (`--body "..."`, `--comment "..."`, `-f body="..."`). The shell can expand backticks (command substitution) and other metacharacters before `gh` ever sees the text. Always write the rendered body to a temp file first and pass it via `--body-file <path>`, `--body-file -` (stdin), or `--field body=@<path>`. For `gh api ... -f body=...`, use `-F body=@<path>` (capital `-F` with `@<path>`) to read from a file.
- One task per branch; one branch per PR; one PR closes exactly one task issue via `Closes #NNN`.
- Branch name = `<prefix>-<NNN>-<slug>` derived from the task issue title (e.g. `wf-impl-028-docs`).
- Branches are always created from a freshly pulled `main`.
- **NEVER** request a new review from `copilot-pull-request-reviewer` (no `gh pr edit --add-reviewer`, no `--request-changes` re-requests).
- **NEVER** run `gh pr checks --watch` in async mode — sync only, generous timeout.
- For every Copilot review comment: (a) implement fix, (b) commit + push, (c) `POST /pulls/{PR#}/comments/{id}/replies` describing the fix, (d) GraphQL `resolveReviewThread`.
- For every failing CI check: read the failing job logs, fix, push, re-watch CI.
- Quality gates **always** run from the package's source root (e.g. `src/services/<svc>`), never from the repo root. Standard sequence: `ruff format . && ruff check . && mypy src tests && pytest -q` (honor the project's documented coverage floor).
- macOS hidden-pth fix on `ModuleNotFoundError` after pip install: `chflags -R nohidden ../../../.venv/lib/python*/site-packages/`.
- Commit messages = Conventional Commits (`<type>(<scope>): <PREFIX>-NNN <summary>`) with body listing changed files + quality summary + `Closes #NNN`.
- Merge mode = `--squash --delete-branch`.
- When the tracker reaches zero `- [ ]` lines: **autonomously close the tracker** with a milestone-summary comment, print a final summary, and stop.
- No destructive git operations (`push --force`, `reset --hard`, `--no-verify`) without explicit user instruction.

---

## Agent Hierarchy Rules

| Level | Role | Spawns |
|---|---|---|
| 1 | Orchestrator (you) | Explore subagent, GitHub-call subagent (optional) |
| 2 | Explore subagent (read-only) | none |
| 2 | GitHub-call subagent | none |

- Orchestrator handles all user interaction, file edits, commits, and terminal commands directly.
- Explore subagent only for non-trivial code archaeology (Phase 1 design reading on a large component, or debugging unfamiliar CI failures).
- GitHub-call subagent only if Phase 2 needs to file many issues in parallel.
- Never spawn subagents for routine file reads/writes.

---

## Core Directories

| Purpose | Path |
|---|---|
| Component designs (source of truth) | `design/components/<component>/` |
| Per-component design doc | `design/components/<component>/design.md` |
| Per-component change records | `design/components/<component>/changes/` |
| Per-component task ledger | `design/components/<component>/todos.md` |
| Per-component implementation plan (created by this skill) | `design/components/<component>/implementation-plan.md` |
| Architecture context | `design/architecture/` |
| Service source code | `src/services/<component>/` |
| Library source code | `src/libs/<component>/` |
| Templates for this skill | `.github/skills/implement-component/templates/` |

---

## Phase 1 — Plan (gate 1)

**Subcommand**: `plan <component>` (also runs when default invocation cannot find an open tracker).

### Inputs

- `design/components/<comp>/design.md` (required).
- Related architecture context under `design/architecture/`.
- Any `design/components/<comp>/changes/` change records.
- Existing source under `src/services/<comp>/` or `src/libs/<comp>/` (to detect what is already built so the plan does not duplicate completed work).

**Do not** use the existing `design/components/<comp>/todos.md` as a seed. The plan is derived fresh from the design documents.

### Procedure

1. Read the component's design end-to-end.
2. Cross-reference dependencies on other components in `design/architecture/`.
3. Scan existing source code to mark what already exists.
4. Break the design into 2–6 sequential **phases**. Typical pattern: scaffold → data model → core logic → integration → tests → docs.
5. For each phase, derive 2–8 **tasks**. Each task has:
   - **Title**: `<PREFIX>-NNN: <imperative summary>` where `<PREFIX>` is derived from the component (e.g. `CAT-IMPL`, `WF-IMPL`, `AUTH-IMPL`).
   - **Scope**: bulleted, concrete files + APIs touched.
   - **Acceptance criteria**: testable bullets.
   - **Depends on**: list of `<PREFIX>-NNN` task ids from earlier phases.
   - **Estimated complexity**: S / M / L (for ordering only — no time estimates).
6. Number tasks sequentially across phases starting from the next free `<PREFIX>-NNN`. Before numbering, query existing GitHub issues with the `component:<comp>` label to find the highest already-used number, then continue from there.
7. Render the plan using `templates/implementation-plan.md` (phases → tasks tree + Mermaid dependency graph).

### Gate

Print the plan to the user. Wait for explicit approval. The user may:
- Add, remove, reorder tasks.
- Change scope or acceptance criteria.
- Split or merge phases.
- Change the `<PREFIX>` if they prefer a different convention.

On approval, save the final plan to `design/components/<comp>/implementation-plan.md` and report the path.

---

## Phase 2 — File issues (gate 2)

**Subcommand**: `file <component>`.

### Inputs

- Approved `design/components/<comp>/implementation-plan.md`.

### Procedure

1. For each task in the approved plan, create a GitHub issue using `templates/task-issue-body.md`:
   - **Title**: `<PREFIX>-NNN: <summary>`.
   - **Labels**: `component:<comp>`, `phase:implementation`, `type:implementation`, plus phase label (`phase:A`, `phase:B`, …).
   - **Body**: `## Summary` + `## Scope` + `## Acceptance Criteria` + `## Depends on` + tracker link placeholder.
2. Create the tracker issue using `templates/tracker-issue-body.md`:
   - **Title**: `<PREFIX>-000-<COMPONENT>: <component> implementation tracker`.
   - **Labels**: `component:<comp>`, `type:tracking`.
   - **Body**: phase headings + checkbox list (`- [ ] #NNN — <PREFIX>-NNN: <summary>`), Mermaid dependency graph, link back to the design doc and implementation plan.
3. Edit each task issue body to insert the actual tracker number (replace the placeholder from step 1).
4. Update `design/components/<comp>/todos.md` to mark each derived task as **filed** with its issue number (e.g. `- [F] WF-IMPL-XXX (#NNN): <summary>`). Use the `[F]` marker so it is visually distinct from done (`[x]`) and pending (`[ ]`).
5. Print a summary table mapping local task id → GitHub issue number → tracker number.

### Gate

Confirm with the user that the issues were filed correctly. On approval, proceed to Phase 3.

---

## Phase 3 — Execute

**Subcommand**: `execute <tracker#>` (also runs when default invocation finds an open tracker).

### Pre-execution prompt

Ask: *"What is the Copilot review wait window for this tracker? (default: 10 minutes)"*. Wait briefly for the user's answer. If they do not answer within a short prompt, proceed with **10 minutes**.

### Loop

Repeat until the tracker has zero `- [ ]` lines:

1. **Pick the next task**: the first `- [ ] #NNN — <PREFIX>-NNN` line whose dependency issues are all CLOSED. Skip blocked tasks but warn if every remaining task is blocked.
2. **Pre-flight**: confirm a clean working tree, on `main`, then `git pull --ff-only origin main`.
3. **Create branch**: `git checkout -b <prefix>-<NNN>-<slug>` (slug derived from the task title).
4. **Read task scope + acceptance criteria** from the issue body.
5. **Implement**: write code + tests + doc updates. Update the component `README.md` (status block, layout, milestone pointer if applicable) and tick the matching task line in `design/components/<comp>/todos.md`.
6. **Quality gates** from the package source root:
   ```
   chflags -R nohidden ../../../.venv/lib/python*/site-packages/ 2>/dev/null || true
   ruff format . && ruff check . && mypy src tests && pytest -q
   ```
   Honor the project's coverage floor (read from `pyproject.toml` or `pytest.ini`). Iterate until green.
7. **Commit + push** using `templates/commit-message.md`:
   - Conventional Commits subject: `<type>(<scope>): <PREFIX>-NNN <short summary>`.
   - Body: changed files, quality summary, ends with `Closes #NNN`.
   - `git push -u origin <branch>`.
8. **Open PR** using `templates/pr-body.md`. Render the body to a temp file first, then pass it via `--body-file` so backticks and other shell metacharacters in the body are not expanded by the shell:
   ```
   # Render the body into /tmp/pr-body.md from templates/pr-body.md with placeholders substituted.
   PAGER=cat gh pr create \
     --title "<PREFIX>-NNN: <summary>" \
     --body-file /tmp/pr-body.md
   ```
9. **Watch CI** (sync, never async):
   ```
   PAGER=cat gh pr checks <PR#> --watch --interval 30 --fail-fast
   ```
   On failure: fetch the failing job's logs (`gh run view --log-failed`), diagnose, fix, push, re-watch.
10. **Wait for Copilot review** — poll `gh api repos/<owner>/<repo>/pulls/<PR#>/reviews` every 60 seconds up to the chosen window (default 10 min). Break early if a review arrives.
11. **Process review threads** — for every unresolved thread:
    1. Apply the fix locally.
    2. Commit (`fix(<scope>): address Copilot review — <short>`).
    3. Push.
    4. Reply on the thread. Render the reply text (which contains backticks around the short SHA) into a temp file first, then pass it via `-F body=@<file>` so the shell cannot expand backticks before `gh` receives it:
       ```
       # Render templates/review-reply.md into /tmp/reply.md with the sha + description substituted.
       PAGER=cat gh api -X POST \
         repos/<owner>/<repo>/pulls/<PR#>/comments/<comment_id>/replies \
         -F body=@/tmp/reply.md
       ```
       Use `templates/review-reply.md` for the reply text.
    5. Resolve the thread with the GraphQL mutation:
       ```graphql
       mutation { resolveReviewThread(input: {threadId: "<id>"}) { thread { isResolved } } }
       ```
    6. Re-watch CI after each push.
12. **Merge**:
    ```
    PAGER=cat gh pr merge <PR#> --squash --delete-branch
    ```
13. **Sync local**: `git checkout main && git pull --ff-only origin main`.
14. **Verify** the task issue moved to `CLOSED`.
15. **Tick the tracker**: pull the tracker body, replace `- [ ] #NNN` with `- [x] #NNN`, push via `gh issue edit <tracker#> --body-file …`.
16. **Loop** back to step 1.

### Stop condition

When no `- [ ] #` lines remain in the tracker body:
1. Compose a milestone-summary comment using `templates/tracker-close-comment.md` (tasks completed, phases delivered, key files added, total commits, total PRs, total review comments addressed). Render the summary into a temp file (e.g. `/tmp/tracker-close.md`) so the multi-line, backtick-bearing Markdown is never inlined into a shell argument.
2. Post the summary as a comment, then close the tracker without re-passing the body:
   ```
   PAGER=cat gh issue comment <tracker#> --body-file /tmp/tracker-close.md
   PAGER=cat gh issue close   <tracker#>
   ```
3. Print the final summary to the user and stop, awaiting further instructions.

---

## Resume semantics

**Subcommand**: `resume` (also runs as the default when a tracker is open).

Detect state in this exact order and act on the first match:

1. **Uncommitted edits on a feature branch** → keep working on the current task; re-run quality gates next.
2. **Open PR with unresolved review threads** → continue thread processing (fix → reply → resolve).
3. **Open PR with all threads resolved but CI red** → fix CI.
4. **Open PR with all threads resolved and CI green** → wait the remaining review window, then merge.
5. **Merged PR but tracker line still unticked** → tick the tracker.
6. **On `main`, tracker has unchecked lines** → pick the next task (step 1 of the execute loop).

---

## Operational reference (commands & patterns)

### `gh` cheat sheet

```
# Reviews on a PR
PAGER=cat gh api repos/<owner>/<repo>/pulls/<PR#>/reviews

# Review threads (id + resolved state + comments)
PAGER=cat gh api graphql -f query='query { repository(owner:"<owner>", name:"<repo>") { pullRequest(number:<PR#>) { reviewThreads(first:50) { nodes { id isResolved comments(first:5) { nodes { databaseId author{login} body path line } } } } } } }'

# Reply to a review comment (read body from file so the shell cannot expand backticks)
PAGER=cat gh api -X POST \
  repos/<owner>/<repo>/pulls/<PR#>/comments/<comment_id>/replies \
  -F body=@/tmp/reply.md

# Resolve a review thread
PAGER=cat gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<id>"}) { thread { isResolved } } }'

# CI watch
PAGER=cat gh pr checks <PR#> --watch --interval 30 --fail-fast

# Failing-job logs
PAGER=cat gh run view <run-id> --log-failed
```

### Tracker tick

```
PAGER=cat gh issue view <tracker#> --json body -q .body > /tmp/<tracker>.md
sed -i.bak 's|^- \[ \] #<NNN> — <PREFIX>-<MMM>|- [x] #<NNN> — <PREFIX>-<MMM>|' /tmp/<tracker>.md
rm /tmp/<tracker>.md.bak
PAGER=cat gh issue edit <tracker#> --body-file /tmp/<tracker>.md
```

### macOS hidden-pth fix

```
chflags -R nohidden ../../../.venv/lib/python*/site-packages/ 2>/dev/null || true
```

Run after every `pip install` or whenever you see `ModuleNotFoundError` during pytest from a fresh shell.

---

## Output contract

At every gate, produce concise, scannable output:

- **Phase 1 output**: phases tree → tasks list → Mermaid dependency graph → "Approve to file as issues?".
- **Phase 2 output**: summary table of `local id → issue # → labels`, tracker URL → "Approve to begin execution?".
- **Phase 3 output**: per task, a short status line — branch, PR URL, CI outcome, review threads handled, merge sha, tracker tick result.
- **Final**: tracker close URL + milestone summary.
