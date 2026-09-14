# GENERATED from habituate/.just/standard.just — do not edit here.
# Re-sync with `habituate sync-recipes`; `habituate repo-doctor` reports drift.

# standard.just — the shared plumbing. Imported, never copied.
#
#     import? '.just/standard.just'
#
# The `?` matters: a checkout without this file still parses, so a fresh clone or an old
# branch is never bricked by a missing import.
#
# WHAT LIVES HERE vs IN YOUR JUSTFILE
#
# just makes a duplicate recipe name a FATAL error across an import -- there is no override
# (verified on just 1.58: `[override]` does not exist, and definition order does not help).
# So this file holds ONLY what no repo needs to redefine: the log helper and the `ready`
# ordering. Your `fmt`, `lint` and `test` stay in your own justfile, and `ready` below calls
# them -- an imported recipe CAN depend on a recipe the importing file defines.
#
# That split is not a compromise, it is the measurement: 11 of 15 repos need a different
# `fmt` body, because containerised Ruby, R packages and mise-pinned toolchains are real
# differences. Generating those centrally is what produced a week of fleet-wide defects.
#
# If you need your own `ready`, drop the import rather than fighting it.

log_dir := "claude_stuff"

# ---------------------------------------------------------------------------
# EVERYTHING THAT MUST BE TRUE BEFORE A COMMIT. Run `just ready`, then commit.
#
# THE ORDER IS LOAD-BEARING. `fmt` APPLIES formatting, so `lint` and `test` run after it --
# anything that ran before was checking code that no longer exists.
#
# Formatting is applied HERE, deliberately, and never by a hook. A formatter that rewrites
# files during a commit changes what you already staged and reviewed, and on a partially
# staged file it will commit half of one. The hook runs `fmt-check`, which only reports:
# together that gives "nothing unformatted is ever committed" without mutating mid-commit.
ready: fmt lint test

# ---------------------------------------------------------------------------
# Run a command, tee it to a timestamped log, and preserve the COMMAND's exit status.
#
#     test: (_logged "test" "bundle exec rake test")
#
# Why: so a failure can be READ instead of reproduced. Re-running a suite to see one error
# message is the thing this exists to stop. Logs are kept, not rotated -- comparing today's
# run to last month's is the point -- and claude_stuff/ is gitignored fleet-wide.
#
# `set -o pipefail` plus ${PIPESTATUS[0]}: a naive `cmd | tee log` reports TEE's status, so a
# failed run exits 0 and the log becomes a lie. This recipe has a bash shebang, so PIPESTATUS
# is right here. It does NOT exist in zsh, where the array is the lowercase, 1-indexed
# `pipestatus` -- do not copy this line into a zsh script.
_logged name +CMD:
    #!/usr/bin/env bash
    set -uo pipefail
    mkdir -p {{ log_dir }}
    log="{{ log_dir }}/{{ name }}-$(date +%Y%m%d-%H%M%S).log"
    {{ CMD }} 2>&1 | tee "$log"
    rc=${PIPESTATUS[0]}
    printf '\nlog: %s (exit %s)\n' "$log" "$rc"
    exit "$rc"
