"""Publish validated playlists to the public git repo (out/ folder).

After EVERY run (any mode), the finished outputs in the local working
``output.dir`` (e.g. ``prod/out/``) are copied into the repo-root ``out/``
folder and committed + pushed to GitHub automatically. The git credential is
read from ``publish.git.auth_file`` (a 2-line file: line1=username,
line2=PAT) and supplied to git via a throwaway GIT_ASKPASS helper so the
secret is never printed or logged.

All steps are best-effort: any failure is reported via the returned dict and
logged, but never raises (a publish problem must not break a validation run).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import datetime
import fcntl
from pathlib import Path


class FileLockish:
    """Minimal advisory file lock (fcntl.flock). No external deps.

    Used to serialize output regeneration across concurrent run processes so a
    short run finishing mid-way through a long full run cannot clobber out/
    simultaneously.
    """
    def __init__(self, path: str, timeout: int = 600):
        self.path = path
        self.timeout = timeout
        self._fd = None

    def acquire(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._fd = open(self.path, "w")
        # blocking flock with a simple deadline
        import time
        deadline = time.time() + self.timeout
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                if time.time() >= deadline:
                    self._fd.close()
                    self._fd = None
                    raise TimeoutError("timed out waiting for publish lock")
                time.sleep(1)

    def release(self):
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                self._fd.close()
            except Exception:
                pass
            self._fd = None


def _resolve(config, dotted: str, default: str) -> str:
    """Resolve a config path key against the config file dir (Option A)."""
    val = config.get(dotted)
    if not val:
        val = default
    if os.path.isabs(val):
        return val
    base = getattr(config, "config_dir", "") or os.getcwd()
    return os.path.normpath(os.path.join(base, val))


def _copy_outputs(src_dir: str, dst_dir: str) -> list:
    """Copy every file in src_dir into dst_dir (overwrite). Returns copied names."""
    copied = []
    os.makedirs(dst_dir, exist_ok=True)
    for entry in os.scandir(src_dir):
        if entry.is_file():
            shutil.copy2(entry.path, os.path.join(dst_dir, entry.name))
            copied.append(entry.name)
    return copied


def _git(args, cwd, auth_file, timeout=120):
    """Run a git command with credential supplied via GIT_ASKPASS.

    Credential source priority:
      1. `auth_file` (2 lines: user, PAT) — legacy/native deployments.
      2. env GITHUB_USER / GITHUB_PAT (Docker / .env model).
    The secret is written to a throwaway askpass helper and never printed/logged.
    """
    askpass = None
    env = dict(os.environ)
    user = os.environ.get("GITHUB_USER", "")
    pat = os.environ.get("GITHUB_PAT", "")
    if auth_file and os.path.isfile(auth_file):
        cred_src = auth_file
        mode = "file"
    elif user and pat:
        cred_src = None
        mode = "env"
    else:
        cred_src = None
        mode = None
    if mode:
        fd, askpass = tempfile.mkstemp(prefix="git_askpass_", suffix=".sh")
        with os.fdopen(fd, "w") as f:
            f.write("#!/bin/bash\n")
            if mode == "file":
                f.write(f'F={cred_src!r}\n')
                f.write('case "$1" in\n')
                f.write('  Username*) sed -n "1p" "$F" | tr -d "[:space:]";;\n')
                f.write('  Password*) sed -n "2p" "$F" | tr -d "[:space:]";;\n')
                f.write('esac\n')
            else:  # env
                import shlex
                f.write('case "$1" in\n')
                f.write(f'  Username*) printf %s {shlex.quote(user)};;\n')
                f.write(f'  Password*) printf %s {shlex.quote(pat)};;\n')
                f.write('esac\n')
        os.chmod(askpass, 0o700)
        env["GIT_ASKPASS"] = askpass
        env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        return subprocess.run(
            ["git"] + args, cwd=cwd, env=env,
            capture_output=True, text=True, timeout=timeout,
        )
    finally:
        if askpass and os.path.exists(askpass):
            os.remove(askpass)


def publish_outputs(config, run_id: str = "", mode: str = "") -> dict:
    """Copy working outputs to the repo out/ folder and push to git.

    `mode` gates publishing: by default refresh runs do NOT publish (they only
    refresh tokens; publishing their partial output would clobber the full
    playlist on GitHub). Pass mode='refresh' to force-skip.

    A file lock serializes regeneration across concurrent runs so a short run
    finishing mid-way through a long full run cannot clobber out/ simultaneously.

    Returns a status dict. Best-effort: errors are captured, never raised.
    """
    result = {
        "published": False, "copied": [], "commit": None,
        "push": None, "error": None, "skipped": None,
    }
    pub = config.get("publish") or {}
    if not pub.get("enabled", True):
        result["skipped"] = "publish disabled in config"
        return result

    # refresh runs publish too (they regenerate the FULL output from the whole DB,
    # so pushing is safe — it is not a partial playlist).
    # 1) resolve source (local working output) and destination (repo out/)
    src = _resolve(config, "output.dir", "./out")
    target_rel = pub.get("target_dir", "../out")
    if os.path.isabs(target_rel):
        dst = target_rel
    else:
        base = getattr(config, "config_dir", "") or os.getcwd()
        dst = os.path.normpath(os.path.join(base, target_rel))

    if not os.path.isdir(src):
        result["error"] = f"source output dir missing: {src}"
        return result

    # serialize output regeneration/push across processes
    lock_path = os.path.join(dst, ".publish.lock")
    lock = FileLockish(lock_path, timeout=600)
    try:
        lock.acquire()
    except TimeoutError:
        result["skipped"] = "another publish in progress; skipped this run"
        return result
    try:
        # 2) copy
        try:
            result["copied"] = _copy_outputs(src, dst)
        except Exception as e:  # noqa: BLE001
            result["error"] = f"copy failed: {e}"
            return result

        # 3) git commit + push
        g = pub.get("git") or {}
        if not g.get("enabled", True):
            result["published"] = True  # copied, no push requested
            result["skipped"] = "git push disabled"
            return result

        repo = g.get("remote", "origin")
        branch = g.get("branch", "main")
        auth_file = g.get("auth_file", os.environ.get("GITHUB_AUTH_FILE", ""))
        # Public / Docker-friendly: the target repo URL can be supplied via the
        # .env file (GITHUB_REPO_URL) or config (publish.git.repo_url) so users
        # don't need a pre-cloned repo with a hardcoded remote. Falls back to an
        # existing local remote when neither is set.
        repo_url = os.environ.get("GITHUB_REPO_URL") or g.get("repo_url") or ""

        # The published repo root: search upward from dst (the out/ folder) for
        # a .git. This works for both layouts:
        #   - native:  repo-root/ has .git, playlists land in repo-root/out/
        #   - docker:  the mounted /out volume IS the repo (clone or auto-init)
        # Falls back to dst itself when no parent .git is found (auto-init case).
        def _find_repo_root(path):
            cur = os.path.abspath(path)
            while True:
                if os.path.isdir(os.path.join(cur, ".git")):
                    return cur
                parent = os.path.dirname(cur)
                if parent == cur:
                    return os.path.abspath(path)
                cur = parent

        repo_root = _find_repo_root(dst)

        if not os.path.isdir(os.path.join(repo_root, ".git")):
            # No local repo yet: auto-init so first run can publish too.
            init = _git(["init", "-q"], repo_root, auth_file)
            if init.returncode != 0:
                result["error"] = "git init failed: " + (init.stderr.strip()[:200])
                return result

        if repo_url:
            # Ensure the remote points at the configured URL (idempotent).
            _git(["remote", "set-url", repo, repo_url], repo_root, auth_file)
            if _git(["remote", "get-url", repo], repo_root, auth_file).returncode != 0:
                _git(["remote", "add", repo, repo_url], repo_root, auth_file)

        if not os.path.isdir(os.path.join(repo_root, ".git")):
            result["error"] = f"not a git repo: {repo_root}"
            return result

        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = f"auto-publish playlists ({run_id or 'run'}) {ts}"

        add = _git(["add", "out/"], repo_root, auth_file)
        if add.returncode != 0:
            result["error"] = "git add failed: " + (add.stderr.strip()[:200])
            return result

        status = _git(["status", "--porcelain", "out/"], repo_root, auth_file)
        if not status.stdout.strip():
            result["published"] = True
            result["skipped"] = "no changes to publish"
            return result

        commit = _git(["commit", "-m", msg], repo_root, auth_file)
        result["commit"] = commit.stdout.strip() or commit.stderr.strip()[:120]
        if commit.returncode != 0:
            result["error"] = "git commit failed: " + (commit.stderr.strip()[:200])
            return result

        push = _git(["push", repo, f"HEAD:{branch}"], repo_root, auth_file)
        result["push"] = "ok" if push.returncode == 0 else push.stderr.strip()[:200]
        if push.returncode != 0:
            result["error"] = "git push failed: " + result["push"]
            return result

        result["published"] = True
        return result
    finally:
        lock.release()



