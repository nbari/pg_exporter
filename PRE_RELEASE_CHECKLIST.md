# Pre-Release Checklist

Complete this checklist before every release to prevent production panics.

## ✅ Local Verification

```bash
# 1. Verify PostgreSQL setup
./scripts/setup-local-test-db.sh

# 2. Run all tests
export PG_EXPORTER_DSN="postgresql://postgres:postgres@localhost:5432/postgres"
just test

# 3. Check for unsafe patterns
git diff main src/collectors/ | grep -E "row\.get\(" | grep -v "try_get" || echo "✓ No unsafe row.get()"

# 4. Verify linting
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

## ✅ Code Review Requirements

### For Any Collector Changes

- [ ] All `row.get()` replaced with `row.try_get()` + fallbacks
- [ ] All SQL numeric columns have explicit type casts (`::bigint`, `::double precision`)
- [ ] Extension availability checked before use
- [ ] NULL values handled gracefully
- [ ] Division by zero prevented

### For New Collectors

- [ ] Registration test exists
- [ ] Extension availability test exists  
- [ ] NULL value handling test exists
- [ ] Type conversion test exists
- [ ] Realistic workload test exists
- [ ] Documented in README.md

## ✅ Testing Requirements

- [ ] All unit tests pass: `cargo test --lib`
- [ ] All integration tests pass: `cargo test --test collectors_tests`
- [ ] CI passes on PostgreSQL 16, 17, 18
- [ ] No clippy warnings
- [ ] Code properly formatted

## ✅ CI/CD Status

- [ ] All GitHub Actions workflows pass
- [ ] Green checkmark on latest commit
- [ ] No flaky tests
- [ ] Coverage report reviewed (if applicable)

## ✅ Documentation

- [ ] CHANGELOG.md updated with changes
- [ ] README.md updated if features added
- [ ] Breaking changes documented
- [ ] New collectors added to tests/TESTING.md examples

## ✅ Final Steps

```bash
# Verify CI status
gh run list --limit 5

# Tag the release
git tag -a v0.x.x -m "Release v0.x.x"
git push origin v0.x.x
```

## ❌ DO NOT RELEASE If:

- Setup script fails: `./scripts/setup-local-test-db.sh`
- Any tests fail
- CI is not green
- Unsafe patterns found: `row.get()` without `try_get()`
- SQL queries lack type casts on NUMERIC columns
- New collectors missing required tests
- Extension availability not checked
- Documentation incomplete

---

## Post-Release Monitoring

- [ ] Monitor error tracking for panics
- [ ] Check Prometheus alerts
- [ ] Review application logs
- [ ] Update docs if issues found

---

## Emergency Hotfix Process

1. Reproduce locally: `./scripts/setup-local-test-db.sh`
2. Write failing test
3. Fix the bug
4. Verify test passes
5. Update tests/TESTING.md with lesson learned
6. Fast-track through CI
7. Deploy

---

**See also:**
- [DEVELOPMENT.md](DEVELOPMENT.md) - Development guide
- [tests/TESTING.md](tests/TESTING.md) - Testing patterns and examples
