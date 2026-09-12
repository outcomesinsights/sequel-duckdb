test:
    bundle exec rake test

lint:
    bundle exec rubocop

ci: fmt-check lint test

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
pre-commit: fmt-check lint test
