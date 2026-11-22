# Release Workflow Tag Hardening (#44) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the `docs.yml` `apt` job — which imports the archive signing key — (and the `build` job) from executing scripts and secrets handling from whatever commit a `v*` tag points at: pin both checkouts to `main` and put the apt job behind an environment the owner can gate.

**Architecture:** On a `v*` tag push, `release.yml`'s `pages` job calls `docs.yml` with `secrets: inherit`, so the reusable workflow runs with the full secret set (`release.yml:51-60`). GitHub resolves same-repo `workflow_call` workflows from the caller's ref — the tag — and both `actions/checkout` steps in `docs.yml` (build `:60`, apt `:90`) pass no `ref:`, so all scripts the apt job runs (`scripts/build-apt-repo.sh`, `scripts/fetch-published-debs.sh`) come from the tagged tree while `APT_GPG_PRIVATE_KEY` (imported at `docs.yml:110-117` before the scripts run) is in the environment. The fix: `ref: main` on both checkouts (matching the documented release policy that only commits `main` has passed may be tagged — `docs/development.md`), plus an `apt-signing` environment on the apt job as the hook for an owner-configured approval gate and environment-scoped signing secret.

**Limits of the pin (do not overstate it in docs or commit messages).** `uses: ./.github/workflows/docs.yml` (`release.yml:57`) loads `docs.yml` *itself* from the tagged commit. So `ref: main` only protects tags on commits that already contain this change; a tag on any older commit, or on a commit whose `docs.yml` drops the pin, still runs that commit's scripts. Likewise a tagged `docs.yml` can drop the `environment:` line. The control that the tagged tree cannot remove is the maintainer moving `APT_GPG_PRIVATE_KEY` from a repository secret to an **environment secret of `apt-signing`**, with deployment rules (only `main` and `v*` tags) and optional required reviewers: a job only sees environment secrets when it declares that environment, and declaring it subjects the run to the environment's rules. GitHub's reusable-workflow rule matters here: environment secrets cannot be passed by the caller (`secrets: inherit` does not carry them); a job in the called workflow that declares `environment:` reads them directly. Hence Task 1 also relaxes `on.workflow_call.secrets.APT_GPG_PRIVATE_KEY` from `required: true` to `required: false` — otherwise a release run fails at call time once the repository-level secret is removed. The job keeps failing closed through its existing `[ -n "$APT_GPG_PRIVATE_KEY" ]` check (`docs.yml:114`). Moving the secret is a repository setting, not code: Task 2 documents it; the implementer does not (and cannot) do it.

**Tech Stack:** GitHub Actions workflow YAML only; verification by YAML parsing and diff reading (no Go changes, no unit tests apply).

**Spec:** https://github.com/bzed/runakefli/issues/44 and its validation comment; `docs/development.md` (the "apt repository and its key" section, including the warning paragraph added in commit `2b7ae14`).

## Global Constraints

- `make check` must still pass before push (nothing in this plan can affect it, but run it before pushing).
- Conventional commits, one logical change per commit.
- Pin only `docs.yml`. `deb.yml` is equally unpinned but holds no secrets, and `check-version` in `release.yml` must intentionally keep validating the tag's own changelog. Do not touch them.
- Only touch the secret declaration of `APT_GPG_PRIVATE_KEY` as Task 1 Step 2b says; the S3/Google secrets stay as they are.
- The existing fail-closed keyring check in the apt job (`gpg --armor --export | diff - apt/runakefli-archive-keyring.asc`, `docs.yml:120`) stays.
- GitHub Actions YAML is indentation-sensitive: two-space indents, exactly as the file does it.

## Review Focus

1. A push-to-`main` run of `docs.yml` (non-release) behaves the same in practice: the default checkout is `github.sha`, `ref: main` is main's tip when the job starts. They differ only when a newer push landed in between (the `pages` concurrency group queues runs, `cancel-in-progress: false`), and then the newer tree is the right one to publish. Two real behaviour changes to accept and document: a release run now publishes docs built from main's tip, not the tagged tree, and a `workflow_dispatch` from a non-`main` branch builds `main`. Confirm by reading that no step in the build/apt jobs references `github.sha`.
2. The `s3` job handles four `S3_*` secrets but checks nothing out (artifact-only) — it must remain untouched.
3. `environment:` goes at job level of `apt`, directly after `runs-on: ubuntu-24.04` (`:82`) and before the comment + `container:` (`:83-84`). The `deploy` job (`:136-138`) uses the long `name:/url:` form; the `apt` job has no URL, so the short form `environment: apt-signing` is correct.
4. Adding `ref: main` must not change `fetch-depth` or tokens: `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1` stays.
5. After this change the documented warning paragraph in `docs/development.md` (added by `2b7ae14`) is stale: it says "neither checkout pins a `ref:`" — Task 2 rewrites it, or the docs lie in the other direction.

---

### Task 1: Pin the checkouts, gate the apt job

**Files:**
- Modify: `CHANGELOG.md` — `## [Unreleased]` → `### Changed`
- Modify: `.github/workflows/docs.yml` — header comment (`:10-12`), `workflow_call` secret `APT_GPG_PRIVATE_KEY` (`:33-34`), build job checkout (`:60`), apt job (`:82-90`)

**Interfaces:**
- Consumes: nothing.
- Produces: a workflow where, from this commit on, no job that holds a secret executes scripts from an unpinned checkout, and where the signing key can live in the `apt-signing` environment.

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

Environments are created automatically on first reference in a workflow, so the `environment:` line itself needs no separate setup; it is inert (no gate, no secrets of its own) until the owner configures it — Task 2 documents that configuration.

- [ ] **Step 2b: Let the signing key come from the environment**

In `on.workflow_call.secrets` (`docs.yml:33-34`), change

```yaml
      APT_GPG_PRIVATE_KEY:
        required: true
```

to

```yaml
      # Optional at call time: the key may be an environment secret of
      # apt-signing (which a caller cannot pass); the apt job fails closed
      # when it is missing from both places.
      APT_GPG_PRIVATE_KEY:
        required: false
```

and in the header comment (`docs.yml:10-12`) replace

```yaml
# Needs, once: Settings > Pages > Source: "GitHub Actions", the secret
# APT_GPG_PRIVATE_KEY (docs/development.md, "The apt repository and its key"), and
# a deployment rule on the github-pages environment that allows main and v* tags.
```

with

```yaml
# Needs, once: Settings > Pages > Source: "GitHub Actions", the secret
# APT_GPG_PRIVATE_KEY as an environment secret of the apt-signing environment
# (docs/development.md, "The apt repository and its key"), and a deployment
# rule on the github-pages and apt-signing environments that allows main and
# v* tags.
```

- [ ] **Step 3: Verify the YAML parses and the pinning is complete**

Run:
```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/docs.yml'))" && \
grep -n -A2 "actions/checkout" .github/workflows/docs.yml && \
grep -n -B1 -A3 "environment: apt-signing" .github/workflows/docs.yml
```
Expected: no parse error; each of the exactly two `actions/checkout` lines is followed by `with:` / `ref: main`; `environment: apt-signing` sits between `runs-on: ubuntu-24.04` and the `container:` comment of the `apt` job. If `pyyaml` is unavailable, verify by reading the diff — indentation copied from the surrounding steps is the risk.

- [ ] **Step 4: Review the whole diff**

Run: `git diff .github/workflows/docs.yml`
Expected: only the two `with: ref: main` blocks with their comments, the `environment: apt-signing` line, the `required: false` change with its comment, and the header-comment wording. No other workflow changes (`release.yml`, `deb.yml` untouched).

- [ ] **Step 5: CHANGELOG**

AGENTS.md wants the changelog line in the same commit as the change. In `CHANGELOG.md` under `## [Unreleased]` → `### Changed` (the section already exists), add:

```markdown
- CI: the `docs.yml` `build` and `apt` jobs check out `main` instead of whatever commit a `v*` tag points at (a release therefore publishes the docs of `main`), and the `apt` job (which imports the archive signing key) runs under the new `apt-signing` environment, which can hold the signing key as an environment secret and gate it with deployment rules and reviewers; `APT_GPG_PRIVATE_KEY` is no longer a required `workflow_call` secret (#44).
```

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/docs.yml CHANGELOG.md
git commit -m "ci: pin docs.yml checkouts to main and gate the apt signing job (#44)"
```

### Task 2: Update the release-security documentation

**Files:**
- Modify: `docs/development.md` — the warning paragraph at lines 82-93, right after "Users must fetch the new public key, so rotate rarely." (`:80`, added in `2b7ae14`), and the "One-time repository settings" list (`:36-55`; the `APT_GPG_PRIVATE_KEY` bullet is `:40`, branch protection `:53`)

**Interfaces:**
- Consumes: Task 1's workflow state (and its CHANGELOG line).
- Produces: documentation matching the workflows; the owner's settings checklist now includes the environment.

- [ ] **Step 1: Rewrite the warning paragraph**

Replace the "**Whoever can push a `v*` tag can run scripts with the signing key.**" paragraph (the whole paragraph; it currently says the checkouts are unpinned) with:

```markdown
**Tag protection is key protection.** In a release run (`v*` tag push) the
`build` and `apt` jobs of `docs.yml` check out `main`, not the tagged commit,
and the `apt` job runs under the `apt-signing` environment. This only helps
for tags on commits that contain it: the workflow files themselves
(`release.yml`, `docs.yml`, `deb.yml`) are always read from the tagged
commit, so a tag on an older commit, or on one whose `docs.yml` drops the
pin, runs that commit's scripts. Keep `APT_GPG_PRIVATE_KEY` as an
environment secret of `apt-signing` (not a repository secret), restrict that
environment's deployments to `main` and `v*` tags, restrict `v*` tag pushes
(Settings → Rules → Rulesets, or tag protection rules), and only ever tag a
commit that `main` has already passed (#44). For a human approval gate
before the key is imported, give `apt-signing` required reviewers.
```

- [ ] **Step 2: Add the environment to the one-time settings checklist**

In `docs/development.md`, in the "One-time repository settings (maintainer, not the implementer)" list, after the branch-protection bullet, add:

```markdown
- Settings → Environments → `apt-signing` (referenced by `docs.yml`'s `apt`
  job): hold `APT_GPG_PRIVATE_KEY` here as an environment secret and delete
  the repository-level secret of that name; deployment branches and tags:
  `main` and `v*` (push-to-`main` runs sign the snapshot suite too);
  optionally required reviewers, for a human approval gate before the
  archive signing key is imported.
```

Also reword the existing `APT_GPG_PRIVATE_KEY` bullet (`:40`, "The secret `APT_GPG_PRIVATE_KEY`, see below.") to "The secret `APT_GPG_PRIVATE_KEY` (an environment secret of `apt-signing`, see below)." 

- [ ] **Step 3: Verify**

Run: `make docs`
Expected: PASS (mkdocs `--strict`; catches stale links in `development.md`). Also read `git diff docs/development.md` to confirm no leftover claim that the checkouts are unpinned.

- [ ] **Step 4: Commit**

```bash
git add docs/development.md
git commit -m "docs: release tag security after the docs.yml hardening (#44)"
```
