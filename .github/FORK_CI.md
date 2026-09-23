# CI on forks

The same unit matrix, Node/Bun runtime integration tests, and Dojo E2E matrix run
on forks using GitHub-hosted runners. The upstream repository retains its Depot
runners. Dojo keeps the upstream AG-UI mock-backed test setup; fork portability
does not disable any test matrix or change its assertions.

The OpenCode adapter workflow uses a local model fixture and needs no paid model
credentials. Its protocol, runtime, and real OpenCode checks run on forks too.
The integration-only CLI is installed with `npm ci` from
`tools/opencode-tests/cli/package-lock.json`, outside the production workspace.

## Package previews

Every matching fork push or PR still runs the full package build. Without external
publishing enabled, CI also runs `pnpm pack` for every non-private package and
uploads the tarballs as a `package-previews-<sha>` Actions artifact (seven-day
retention). Build, pack, and artifact-upload errors fail the check.

To publish fork previews through pkg.pr.new, install the `pkg-pr-new` GitHub app
on the fork, then set the repository Actions variable `PKG_PR_NEW_ENABLED` to
`true`. The upstream repository publishes by default. Once publishing is enabled,
service/authentication/publish errors remain failures; there is no error fallback.

Without the app, only external publishing is omitted. Package build and packing
remain required, and the workflow summary explicitly identifies the artifact-only
result rather than claiming that a pkg.pr.new release was published.
