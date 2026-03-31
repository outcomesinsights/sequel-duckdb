test:
    bundle exec rake test

lint:
    bundle exec rubocop

ci: lint test

bundle-update *ARGS:
    bundle update {{ARGS}}
