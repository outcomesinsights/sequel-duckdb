import? '.just/standard.just'

# Logged, so a failure can be READ rather than reproduced.
test: (_logged "test" "just _test")

_test:
    bundle exec rake test

lint:
    bundle exec rubocop

ci: fmt-check lint test hygiene

bundle-update *ARGS:
    bundle update {{ ARGS }}

# Rewrite files to canonical format. Run deliberately; never from a hook.
fmt:
    git ls-files "*.sh" | xargs -r shfmt -w
    just --fmt --unstable
    git ls-files "*.md" | xargs -r mdformat

# Report format drift without changing anything. This is what the hooks run —
# a formatter that rewrites files mid-commit changes what you already reviewed.
fmt-check:
    git ls-files "*.sh" | xargs -r shfmt -d
    just --fmt --check --unstable
    git ls-files "*.md" | xargs -r mdformat --check

# What actually runs before a push. Defaults to the complete `ci`; point it at
# something smaller ONLY where running complete CI locally is impractical.
pre-push: ci

# Runs on every commit, so it must stay FAST — a sub-minute budget. Tests belong
# here when they fit; lint alone when they do not. fmt-check never rewrites.
pre-commit: fmt-check lint test hygiene

# Content checks inherited from overcommit when it was removed (2026-09-12):
# MergeConflicts, YamlSyntax, JsonSyntax. RuboCop and the test target were already
# covered by fmt-check/lint/test; HardTabs and TrailingWhitespace were dropped because
# they fight shfmt, .tsv, and generated files. See habituate/standards.md.
hygiene:
    #!/usr/bin/env bash
    set -uo pipefail
    rc=0
    bad=$(git ls-files | xargs -r grep -IlE '^(<{7}|={7}|>{7})( |$)' 2>/dev/null || true)
    [ -n "$bad" ] && { echo "merge conflict markers:"; printf '%s\n' "$bad" | sed 's/^/  /'; rc=1; }
    for f in $(git ls-files '*.yml' '*.yaml'); do
      python3 -c 'import yaml,sys; yaml.safe_load(open(sys.argv[1]))' "$f" 2>/dev/null \
        || { echo "invalid YAML: $f"; rc=1; }
    done
    for f in $(git ls-files '*.json'); do
      jq empty "$f" 2>/dev/null || { echo "invalid JSON: $f"; rc=1; }
    done
    exit $rc

# Arm THIS clone's git hooks. A gate in .git/hooks is per-clone and untracked, so a
# fresh clone silently has none; this recipe is the tracked declaration that one is
# expected, plus the installer. `habituate repo-doctor` reports an unarmed clone.
# Chains the beads hook first and preserves any hook it did not write.
hooks:
    #!/usr/bin/env bash
    set -euo pipefail
    root="$(git rev-parse --show-toplevel)"
    # RESOLVE FROM THE GIT DIR, NOT `--git-path hooks`. --git-path HONOURS core.hooksPath,
    # which beads points at .beads/hooks -- so the obvious call aimed this recipe at beads'
    # OWN shim and wrote the dispatcher over it. The dispatcher then chains
    # "$root/.beads/hooks/$h", i.e. itself, and every commit recursed until the shell gave
    # up. Measured by the seeds session 2026-09-13: `git commit` hung silently, reporting
    # only "shell level (1000) too high", until killed at two minutes. Their fix, adopted
    # here verbatim; the guard below is the half that matters, turning a silent self-call
    # into a loud refusal.
    hooks="$(git rev-parse --absolute-git-dir)/hooks"
    case "$hooks" in
        */.beads/hooks)
            echo "refusing to write hooks inside .beads: $hooks" >&2
            echo "core.hooksPath still points at beads; unset it first" >&2
            exit 1
            ;;
    esac
    # A linked worktree shares the git COMMON dir, so arming from one would reach OUT of it
    # and mv the main checkout's live hooks aside while it is using them. One arming covers
    # every worktree, which is why this refuses rather than "fixes" the path.
    if [ "$(git rev-parse --git-common-dir)" != "$(git rev-parse --git-dir)" ]; then
        echo "refusing: run 'just hooks' in the MAIN checkout, not a worktree." >&2
        echo "  hooks are shared via the git common dir, so arming there covers this worktree too." >&2
        exit 1
    fi
    mkdir -p "$hooks"
    for h in pre-commit pre-push; do
        just --summary 2>/dev/null | tr ' ' '\n' | grep -qx "$h" || continue
        live="$hooks/$h"
        # DEFER TO THE BEADS PUSH DRIVER. It already runs the beads shim, then `just
        # pre-push`, then `bd dolt push` -- a superset of this wrapper. Preserving and
        # chaining it instead makes the gate run TWICE per push (measured: ~14 min rather
        # than ~7 in a Spark-backed repo) and fires the beads hook twice. Neither piece is
        # wrong alone; the interaction is.
        if grep -qs 'beads-install-push-driver' "$live"; then
            echo "kept: $h (beads push driver already runs this gate)"
            continue
        fi
        # Never clobber a hook this recipe did not write; chain it instead.
        keep=""
        if [ -f "$live" ] && ! grep -qs 'just hooks' "$live"; then
            mkdir -p "$hooks/preserved"
            keep="$hooks/preserved/$h"
            [ -e "$keep" ] || { mv "$live" "$keep"; chmod +x "$keep"; }
        fi
        {
            echo '#!/usr/bin/env sh'
            echo '# Written by `just hooks`. Re-run to regenerate.'
            echo 'set -e'
            echo 'root="$(git rev-parse --show-toplevel)"'
            echo 'hooks="$(git rev-parse --absolute-git-dir)/hooks"'
            [ -n "$keep" ] && echo "p=\"\$hooks/preserved/$h\"; [ -x \"\$p\" ] && { \"\$p\" \"\$@\" || exit \$?; }"
            echo "b=\"\$root/.beads/hooks/$h\"; [ -x \"\$b\" ] && { \"\$b\" \"\$@\" || exit \$?; }"
            echo "cd \"\$root\" && exec just $h"
        } > "$live"
        chmod +x "$live"
        echo "armed: $h"
    done
