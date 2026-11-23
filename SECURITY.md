# Security policy

Only the latest release and `main` are supported.

## Reporting a vulnerability

Do not open a public issue. Report the vulnerability privately, either

- through GitHub: <https://github.com/bzed/runakefli/security/advisories/new>, or
- by e-mail to bernd@bzed.de.

Include the version (`runakefli -version`), the configuration keys involved
(without secrets) and a way to reproduce it. You get an answer within one
week. Fixes are released as soon as they are ready. They are credited in
[`CHANGELOG.md`](CHANGELOG.md) unless you prefer otherwise.

## Scope

In scope: the Rúnakefli binary, its HTTP API, the Debian package, the container
image and the apt repository. Vulnerabilities in Loki or ClickHouse
themselves belong to those projects. If Rúnakefli reuses the affected Loki code
at its pinned commit (`LOKI_COMMIT` in the `Makefile`), tell us as well.

## Verifying downloads

- The apt repository is signed; the key fingerprint is in the
  [installation guide](docs/install/index.md).
- Release `.deb` files come with `SHA256SUMS`.
- Container images are signed with cosign (keyless, GitHub OIDC) and carry an
  SBOM and provenance attestation:

  ```console
  $ cosign verify ghcr.io/bzed/runakefli:<version> \
      --certificate-identity-regexp '^https://github.com/bzed/runakefli/\.github/workflows/' \
      --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```
