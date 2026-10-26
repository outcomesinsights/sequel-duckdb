# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work atomically
bd close <id>         # Complete work
bd dolt push          # Push beads data to remote
```

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

Shell commands like `cp`, `mv`, and `rm` may be aliased to include `-i` (interactive) mode on some systems, causing the agent to hang indefinitely waiting for y/n input.

**Use these forms instead:**

```bash
# Force overwrite without prompting
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file

# For recursive operations
rm -rf directory            # NOT: rm -r directory
cp -rf source dest          # NOT: cp -r source dest
```

**Other commands that may prompt:**

- `scp` - use `-o BatchMode=yes` for non-interactive
- `ssh` - use `-o BatchMode=yes` to fail instead of prompting
- `apt-get` - use `-y` flag
- `brew` - use `HOMEBREW_NO_AUTO_UPDATE=1` env var

## DuckDB library versions

sequel-duckdb supports the DuckDB C library 1.5.x and 2.x until 1.5.x reaches
end of life in March 2027 (Ryan, 2026-10-26). Production uses 1.5.x, because
DuckDB 2.x has performance regressions. After March 2027, drop 1.5.x: the
deferred bead sequel-duckdb-3oi ("Drop DuckDB 1.5.x support") lists what to remove.

The gemspec does not depend on the duckdb gem, so each project picks its own
duckdb gem version. `Gemfile` uses the duckdb gem 2.x, which builds against
library 1.4 or newer. `gemfiles/duckdb-1.5.gemfile` holds the gem at 1.5.x for
projects that still pin it.

CI (`.github/workflows/main.yml`) runs every Ruby on library 1.5.x with gem 2.x,
one job on library 2.x, and one job with gem 1.5.x. Locally, `just test` uses
the library that the duckdb gem was built against.
`BUNDLE_GEMFILE=gemfiles/duckdb-1.5.gemfile just test` runs gem 1.5.x, and
`just test-libduckdb DIR` builds the gem against another library and runs the
tests. The official 2.x Linux build needs an x86-64-v3 CPU (AVX2), so on an
older CPU, DIR must be a build from source.

When the two versions behave differently, write a test for each version that
skips on the other one. Do not change a DuckDB setting for the whole connection
to make 2.x behave like 1.5.x.

<!-- BEGIN BEADS INTEGRATION profile:full hash:d4f96305 -->

## Releases and Commit Messages

Releases are tag-based and cut weekly by a release agent; nothing about them is
manual for a human. `.github/workflows/release.yml` publishes to rubygems.org
via trusted publishing when a `v*` tag is pushed. To cut one:

1. Update the gems. Run `bundle update` and
   `BUNDLE_GEMFILE=gemfiles/duckdb-1.5.gemfile bundle update`, then `just ci`
   and `BUNDLE_GEMFILE=gemfiles/duckdb-1.5.gemfile just test`. Commit both
   lockfiles as `chore(deps): update gems`. Do not use `build:`, because
   git-cliff counts it toward the version bump. `bundle update` stays inside
   the constraints in `Gemfile` and the gemspec. Then run `bundle outdated`.
   Each gem that it still lists is held back by a constraint. If the
   constraint is in `Gemfile` or the gemspec (the "Requested" column), file a
   bead to raise it, unless an open bead already covers it. A constraint in
   another gem, such as `rubocop-ast` holding back `prism`, is not ours to
   change. The release does not wait for those beads. If `just ci` fails after
   the update, the release waits until a fix is on main.
2. Pick the version with `git-cliff --bumped-version` (configured in
   `cliff.toml`). While the gem is 0.x a breaking change bumps the minor
   version and anything else bumps the patch. If it prints the latest `v*` tag
   unchanged ("There is nothing to bump"), there is nothing to release.
3. Set `VERSION` in `lib/sequel/duckdb/version.rb`, run `bundle install` so
   `Gemfile.lock` agrees, and add the section printed by
   `git-cliff --unreleased --bump` to the top of `CHANGELOG.md`, below its
   header.
4. Land that commit on main and wait for the CI workflow (`main.yml`) to go
   green on the push to main.
5. Push the matching tag, e.g. `git tag v0.3.0 && git push origin v0.3.0`.
   The workflow refuses a tag that does not equal `v` + `VERSION`, a commit
   that is not on main, or one without a green `main.yml` run from a push to
   main. It confirms rubygems.org serves the version afterwards. `v*` tags
   cannot be moved or deleted once pushed (repository ruleset).

Commit types decide what reaches the changelog:

- `feat:` and `fix:` are ONLY for changes to the shipped gem: what a consumer
  of sequel-duckdb would notice.
- Tooling, CI, hooks, the justfile, lint and coverage config, and dependency
  bumps are `chore:`, `ci:` or `build:`. Test-only changes are `test:`.
- A breaking change to the gem's API needs `!` after the type or a
  `BREAKING CHANGE:` footer.

## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**

- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

<!-- END BEADS INTEGRATION -->
