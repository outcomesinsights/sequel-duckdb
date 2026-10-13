test:
    bundle exec rake test

# Installs the pinned tools (mise.toml) and the bundle, then arms the tracked hooks
# (.githooks, gator's githooks standard). A clone needs git and mise. bd bootstrap,
# never bd init: in a clone with .beads, init re-points the hooks at an absolute
# .beads/hooks and commits. Relative, so a worktree runs its own; repeating it changes
# nothing, and it repairs the absolute path bd writes on `bd hooks install`. Guarded:
# pointing git at a missing directory would turn every hook off.
setup:
    mise install
    bundle install
    @if [ -d .beads ] && [ ! -d .beads/embeddeddolt ] && command -v bd >/dev/null 2>&1; then bd bootstrap; fi
    @if [ -d .githooks ]; then git config core.hooksPath .githooks; fi

# Every non-rewriting check. `fmt` rewrites; this only reports. Tools come from
# mise.toml; a missing one fails the recipe.
lint:
    bundle exec rubocop
    actionlint
    zizmor --offline --config .github/zizmor.yml .
    cog check --from-latest-tag --ignore-merge-commits

# The full CI equivalent: GitHub Actions runs these recipes, and the standard
# .githooks/pre-push runs `just ci` at the exact commit being pushed, in a gate
# worktree, before every push.
ci: fmt-check lint test hygiene

# The commit gate. The standard .githooks/pre-commit runs this on the working tree.
# Must stay fast: it runs on every commit.
pre-commit: fmt-check lint seeds-check

# Verify the seed store is plausible and has not been rewritten underneath us.
# `--against-git` compares the committed corpus with the working tree. It is on the
# commit path and NOT in `ci`: a commit is the only way a seed file reaches history,
# and CI runners have no seeds installed. Skips when seeds is absent, so a clone
# without the tool can still commit. treefmt.toml excludes `.seeds/**` for the same
# reason: a formatter must never rewrite the store.
seeds-check:
    @command -v seeds >/dev/null 2>&1 || exit 0; seeds check --against-git

bundle-update *ARGS:
    bundle update {{ ARGS }}

# Formats every tracked file in every language here (treefmt.toml), including
# RuboCop's safe autocorrections.
fmt:
    treefmt

# Fails if `fmt` would change anything. It formats the tree first and THEN fails
# (fix-and-fail): re-stage what it changed. It never rewrites and succeeds.
fmt-check:
    treefmt --fail-on-change

# Content checks inherited from overcommit when it was removed (2026-09-12):
# MergeConflicts, YamlSyntax, JsonSyntax. RuboCop and the test target were already
# covered by fmt-check/lint/test; HardTabs and TrailingWhitespace were dropped because
# they fight shfmt, .tsv, and generated files.
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
