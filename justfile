test:
    bundle exec rake test

# Installs the pinned tools (mise.toml) and the bundle. A clone needs git and mise.
setup:
    mise install
    bundle install

# Every non-rewriting check. `fmt` rewrites; this only reports. Tools come from
# mise.toml; a missing one fails the recipe.
lint:
    bundle exec rubocop
    actionlint
    zizmor --offline --config .github/zizmor.yml .
    cog check --from-latest-tag --ignore-merge-commits

# The full CI equivalent: GitHub Actions runs these recipes, and the tracked
# .beads/hooks/pre-push runs `just ci` before every push. The commit-stage gate is
# .pre-commit-config.yaml (fmt-check on staged files + lint), run by prek from
# .beads/hooks/pre-commit.
ci: fmt-check lint test hygiene

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
