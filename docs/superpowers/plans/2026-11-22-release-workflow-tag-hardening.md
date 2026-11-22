# Release Workflow Tag Hardening (#44) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the `docs.yml` `apt` job — which imports the archive signing key — (and the `build` job) from executing scripts and secrets handling from whatever commit a `v*` tag points at: pin both checkouts to `main` and put the apt job behind an environment the owner can gate.

**Architecture:** On a `v*` tag push, `release.yml`'s `pages` job calls `docs.yml` with `secrets: inherit`, so the reusable workflow runs with the full secret set (`release.yml:51-60`). GitHub resolves same-repo `workflow_call` workflows from the caller's ref — the tag — and both `actions/checkout` steps in `docs.yml` (build `:60`, apt `:90`) pass no `ref:`, so all scripts the apt job runs (`scripts/build-apt-repo.sh`, `scripts/fetch-published-debs.sh`) come from the tagged tree while `APT_GPG_PRIVATE_KEY` (imported at `docs.yml:110-117` before the scripts run) is in the environment. The fix: `ref: main` on both checkouts (matching the documented release policy that only commits `main` has passed may be tagged — `docs/development.md`), plus an `apt-signing` environment on the apt job as the hook for an owner-configured approval gate.

**Tech Stack:** GitHub Actions workflow YAML only; verification by YAML parsing and diff reading (no Go changes, no unit tests apply).

**Spec:** https://github.com/bzed/runakefli/issues/44 and its validation comment; `docs/development.md` (the "apt repository and its key" section, including the warning paragraph added in commit `2b7ae14`).

## Global Constraints

- `make check` must still pass before push (nothing in this plan can affect it, but run it before pushing).
- Conventional commits, one logical change per commit.
- Pin only `docs.yml`. `deb.yml` is equally unpinned but holds no secrets, and `check-version` in `release.yml` must intentionally keep validating the tag's own changelog. Do not touch them.
- The existing fail-closed keyring check in the apt job (`gpg --armor --export | diff - apt/runakefli-archive-keyring.asc`, `docs.yml:120`) stays.
- GitHub Actions YAML is indentation-sensitive: two-space indents, exactly as the file does it.

## Review Focus

1. A push-to-`main` run of `docs.yml` (non-release) must behave identically: with `ref: main` the checkout is the same commit the workflow would have picked by default, so the only behavior change is on tag-triggered runs — which is the point. Confirm by reading that no step in the build/apt jobs references `github.sha`.
2. The `s3` job handles four `S3_*` secrets but checks nothing out (artifact-only) — it must remain untouched.
3. The `apt` job's container (`debian:trixie`) precedes steps; `environment:` goes at job level (before `container:` or after `runs-on:` — match the style of the `deploy` job at `:136`).
4. Adding `ref: main` must not change `fetch-depth` or tokens: `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1` stays.
5. After this change the documented warning paragraph in `docs/development.md` (added by `2b7ae14`) is stale: it says "neither checkout pins a `ref:`" — Task 2 rewrites it, or the docs lie in the other direction.

---

### Task 1: Pin the checkouts, gate the apt job

**Files:**
- Modify: `.github/workflows/docs.yml` — build job checkout (`:60`), apt job (`:82-90`)

**Interfaces:**
- Consumes: nothing.
- Produces: a workflow where no job that holds a secret executes scripts from an unpinned checkout.

- [ ] **Step 1: Pin the `build` job's checkout**

At `.github/workflows/docs.yml` line 60, change:

```yaml
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
```

to:

```yaml
      # Always build from reviewed main, not from whatever commit called this
      # workflow: a v* tag run must not execute stale or unreviewed docs code.
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          ref: main
```

- [ ] **Step 2: Gate and pin the `apt` job**

In the `apt` job: add `environment: apt-signing` at job level (after `runs-on: ubuntu-24.04`, mirroring how the `deploy` job carries `environment:`), and pin its checkout (line 90) the same way as Step 1, with a comment that states the security reason explicitly:

```yaml
      # The signing key is imported below; the scripts it runs must come from
      # reviewed main, never from whatever commit a v* tag points at (#44).
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          ref: main
```

Environments are created automatically on first reference in a workflow, so the `environment:` line itself needs no separate setup; it is inert (no gate) until the owner configures it — Task 2 documents the optional required reviewers.

- [ ] **Step 3: Verify the YAML parses and the pinning is complete**

Run:
```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/docs.yml'))" && \
grep -n "actions/checkout" .github/workflows/docs.yml
```
Expected: no parse error, and every `actions/checkout` in `docs.yml` (there are exactly two) is followed by `ref: main`. If `pyyaml` is unavailable, verify by reading the diff — indentation copied from the surrounding steps is the risk.

- [ ] **Step 4: Review the whole diff**

Run: `git diff .github/workflows/docs.yml`
Expected: only the two `with: ref: main` blocks, the two comment lines, and the `environment: apt-signing` line. No other workflow changes.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/docs.yml
git commit -m "ci: pin docs.yml checkouts to main and gate the apt signing job (#44)"
```

### Task 2: Update the release-security documentation and changelog

**Files:**
- Modify: `docs/development.md` — the warning paragraph after "Users must fetch the new public key, so rotate rarely." (added in `2b7ae14`) and the "One-time repository settings" list
- Modify: `CHANGELOG.md` — `## [Unreleased]`

**Interfaces:**
- Consumes: Task 1's workflow state.
- Produces: documentation matching the workflows; the owner's settings checklist now includes the environment.

- [ ] **Step 1: Rewrite the warning paragraph**

Replace the "**Whoever can push a `v*` tag can run scripts with the signing key.**" paragraph (the whole paragraph; it currently says the checkouts are unpinned) with:

```markdown
**Tag protection is key protection.** In a release run (`v*` tag push) the
`build` and `apt` jobs of `docs.yml` check out `main`, not the tagged commit,
and the `apt` job runs under the `apt-signing` environment. The signing key is
only ever handed to scripts from reviewed `main` — but the release workflow
itself (`release.yml`, `deb.yml`, tag validation) still runs from the tag, so
restrict `v*` tag pushes (Settings → Tags → tag protection rules) and only
ever tag a commit that `main` has already passed (#44). To add a human approval
gate before the key is imported, give the `apt-signing` environment
(Settings → Environments) required reviewers.
```

- [ ] **Step 2: Add the environment to the one-time settings checklist**

In `docs/development.md`, in the "One-time repository settings (maintainer, not the implementer)" list, after the branch-protection bullet, add:

```markdown
- Settings → Environments → `apt-signing` (referenced by `docs.yml`'s `apt`
  job): optional required reviewers for a human approval gate before the
  archive signing key is imported on a release.
```

- [ ] **Step 3: CHANGELOG**

In `CHANGELOG.md` under `## [Unreleased]` → `### Changed`, add:

```markdown
- CI: the `docs.yml` `build` and `apt` jobs check out `main` instead of whatever commit a `v*` tag points at, and the `apt` job (which imports the archive signing key) runs under the new `apt-signing` environment, so a tag on an unreviewed commit can no longer run scripts with signing-key access (#44).
```

- [ ] **Step 4: Verify**

Run: `make docs`
Expected: PASS (mkdocs `--strict`; catches stale links in `development.md`). Also read `git diff docs/development.md` to confirm no leftover claim that the checkouts are unpinned.

- [ ] **Step 5: Commit**

```bash
git add docs/development.md CHANGELOG.md
git commit -m "docs: release tag security after the docs.yml hardening (#44)"
```
