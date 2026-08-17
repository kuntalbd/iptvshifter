"""Test-suite guard: never let a test push to a real git repo.

The example config (examples/config.example.yaml) now ships publish DISABLED,
but as defense-in-depth we also strip any GITHUB_* credentials from the env at
test startup. publish.py reads GITHUB_USER/G GITHUB_PAT/ GITHUB_REPO_URL to
authenticate a push; with them gone, even a misconfigured test run fails the
push gracefully (captured, not raised) instead of publishing to GitHub.
"""
import os

for _v in ("GITHUB_USER", "GITHUB_PAT", "GITHUB_REPO_URL", "GITHUB_AUTH_FILE"):
    os.environ.pop(_v, None)
