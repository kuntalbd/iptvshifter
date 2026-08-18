"""Stream validation engine (§4.1, §18.2, F1/F4/F12/F17, C1 hybrid).

- Scheme branching: http/https -> HEAD then GET Range; non-HTTP -> uncheckable.
- Embedded headers merged from vlc/http/kodi/pipe (F1/F14/F17).
- Stale-token handling (C1): on 403 of a tokened URL, signal suspected-expired
  so the orchestrator can re-extract a fresh token before blacklisting.
- Per-host semaphore (§4.3) and retry/backoff (§4.2).
"""
from __future__ import annotations

import time
import socket
import threading
import os
import json
import shutil
import faulthandler  # SEGV/native crashes dump a C-level trace instead of silent death

# Enable faulthandler so any native crash in the TLS/DNS stack (which Python
# try/except cannot catch) leaves a Stack trace in the journal instead of a
# bare "code=dumped, signal=SEGV". This is what lets us diagnose the
# 2026-08-17 quick-run SEGV rather than guess.
try:
    faulthandler.enable()
except Exception:
    pass
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

from .logging_utils import get_logger as _get_logger, configure_logging

_time = time

from .utils import merge_headers

# --- DNS timeout guard -------------------------------------------------------
# `socket.getaddrinfo` has NO built-in timeout in requests/urllib3. With many
# concurrent workers hitting hostile/blackhole DNS hosts, threads hang forever
# in getaddrinfo -> the whole run deadlocks. Patch getaddrinfo so every DNS
# lookup is bounded by DNS_TIMEOUT seconds.
#
# CRITICAL: use a BOUNDED thread pool (not a thread-per-call). A thread-per-call
# design spawns one daemon thread per lookup; with thousands of blackhole hosts
# each lookup lives 30-75s (system resolver), so threads pile up unbounded,
# overwhelm the resolver, and eventually hit the OS thread limit -> deadlock.
# A bounded pool caps concurrent DNS at DNS_POOL (default 16) so the resolver is
# never overwhelmed and lookups stay responsive. On timeout we raise so urllib3
# treats it as a retryable connection error; at most DNS_POOL orphaned lookups
# can linger.
_DNS_TIMEOUT = float(__import__("os").environ.get("M3U_DNS_TIMEOUT", "6"))
_DNS_POOL = int(__import__("os").environ.get("M3U_DNS_POOL", "16"))
_orig_getaddrinfo = socket.getaddrinfo
_dns_executor = ThreadPoolExecutor(max_workers=_DNS_POOL, thread_name_prefix="dns")


def _getaddrinfo_timeout(*args, **kwargs):
    fut = _dns_executor.submit(_orig_getaddrinfo, *args, **kwargs)
    try:
        return fut.result(timeout=_DNS_TIMEOUT)
    except Exception:
        # surface as a socket timeout so urllib3/requests treat it as a
        # retryable connection error (validator's except path handles it)
        raise socket.timeout(f"DNS lookup timed out after {_DNS_TIMEOUT}s")


socket.getaddrinfo = _getaddrinfo_timeout

NON_HTTP_SCHEMES = {"rtmp", "rtsp", "mmsh", "mms", "srt", "udp", "rtmpe", "rtmpt"}

# Outcome classes returned by validate_one
class Result:
    __slots__ = ("url", "ok", "status", "reason", "uncheckable", "suspected_expired",
                 "elapsed_ms", "throughput_kbps", "health_score", "health_tier")
    def __init__(self):
        self.url = ""
        self.ok = False
        self.status = 0
        self.reason = ""
        self.uncheckable = False
        self.suspected_expired = False
        self.elapsed_ms = 0.0
        self.throughput_kbps = 0.0
        self.health_score = None
        self.health_tier = None


def _headers_for(stream, fallback_ua: str | None = None) -> dict:
    """Merge embedded headers (player-independent) into a requests header dict."""
    canonical = merge_headers(
        stream.attributes.get("vlc_options", {}),
        stream.attributes.get("http_options", {}),
        stream.attributes.get("pipe_headers", {}),
        stream.attributes.get("kodi_headers", {}),
    )
    out = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    # map canonical -> http header
    if "User-Agent" in canonical:
        out["User-Agent"] = canonical["User-Agent"]
    elif fallback_ua:
        out["User-Agent"] = fallback_ua
    if "Referer" in canonical:
        out["Referer"] = canonical["Referer"]
    if "Origin" in canonical:
        out["Origin"] = canonical["Origin"]
    if "Cookie" in canonical:
        out["Cookie"] = canonical["Cookie"]
    return out


class StreamValidator:
    """Validates streams concurrently. The HTTP transport is injectable
    (`http_client`) so tests can run offline against canned responses."""

    def __init__(self, config, http_client=None, workers=None, retries=None,
                 backoff=None, per_host=None):
        self.config = config
        self.workers = int(workers if workers is not None else config.get("validation.workers", 20))
        self.timeout = (
            int(config.get("validation.timeout_connect", 10)),
            int(config.get("validation.timeout_read", 15)),
        )
        self.retries = int(retries if retries is not None else config.get("validation.retries", 2))
        self.backoff = backoff if backoff is not None else config.get("validation.backoff", [5, 15, 30])
        self.per_host = int(per_host if per_host is not None else config.get("validation.per_host_limit", 5))
        self.verify_ssl = bool(config.get("validation.verify_ssl", True))
        self.follow_redirects = bool(config.get("validation.follow_redirects", True))
        self._client = http_client  # callable(session, method, url, **kw) -> resp-like
        self._session = None
        self._thread_local = __import__("threading").local()
        self._host_sems = {}
        # Global cap on CONCURRENT in-flight network calls (DNS + TLS + transfer).
        # Even with workers=150, firing 150 simultaneous DNS/TLS lookups
        # overwhelms the resolver and hangs threads in getaddrinfo (no timeout
        # there) -> deadlock. The semaphore bounds real concurrency to a safe
        # level; extra worker threads simply queue for a slot.
        self.max_concurrent = int(config.get("validation.max_concurrent", 24))
        # Hard wall-clock cap per link. The (connect,read) request timeout resets
        # on every successful byte, so a server that trickles 1 byte / few-seconds
        # NEVER trips it and a handful of such links stall as_completed forever
        # (the whole run hangs). A per-link daemon-thread deadline caps total time
        # no matter WHERE it stalls (DNS, TLS handshake, or slow-trickle body).
        self.hard_timeout = float(config.get("validation.hard_timeout", 20))
        # Isolate network I/O in a child process so a native SEGV in the
        # TLS/DNS stack cannot kill the whole pipeline (default True).
        self.isolate = bool(config.get("validation.isolate_subprocess", True))
        # Quality / health checking (A + B), independently toggleable
        q = config.get("quality", {}) or {}
        self.latency_check = bool(q.get("latency_check", True))
        self.healthy_max_ms = float(q.get("healthy_max_ms", 2000))
        self.medium_max_ms = float(q.get("medium_max_ms", 5000))
        self.throughput_check = bool(q.get("throughput_check", True))
        self.throughput_sample_seconds = float(q.get("throughput_sample_seconds", 3))
        self.throughput_min_kbps = float(q.get("throughput_min_kbps", 500))
        self._ua_pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "VLC/3.0.20 LibVLC/3.0.20",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
        ]
        self._net_sem = threading.Semaphore(self.max_concurrent)

    # --- transport ---
    def _get_session(self):
        if self._client:
            return None
        # IMPORTANT: requests.Session is NOT thread-safe. validate_batch runs
        # validate_one across a 150-worker thread pool, so a single shared
        # session deadlocks/hangs (esp. the streaming throughput sample in
        # Phase 2). Use a thread-local session: each worker thread gets its own
        # pooled session. This preserves connection-pool efficiency while being
        # safe under concurrency.
        local = getattr(self._thread_local, "session", None)
        if local is None:
            import requests
            from requests.adapters import HTTPAdapter
            s = requests.Session()
            # pool_maxsize must be >= max_concurrent so the connection pool is
            # never exhausted (an exhausted pool makes worker threads block and
            # pile up native TLS handles, a suspected SEGV contributor).
            pool = max(int(self.max_concurrent), 24)
            s.mount("https://", HTTPAdapter(pool_connections=pool, pool_maxsize=pool))
            s.mount("http://", HTTPAdapter(pool_connections=pool, pool_maxsize=pool))
            self._thread_local.session = s
            local = s
        return local

    def _do_request(self, method, url, headers):
        # NOTE: the global concurrency semaphore is acquired by the CALLER
        # (validate_batch's pool worker), NOT here, so a native stall inside
        # this function can never permanently hold the semaphore (which would
        # exhaust it and deadlock the whole run). See validate_batch.
        if self._client:
            return self._client(method, url, headers=headers, timeout=self.timeout,
                                verify=self.verify_ssl, allow_redirects=self.follow_redirects)
        s = self._get_session()
        return s.request(method, url, headers=headers, timeout=self.timeout,
                         verify=self.verify_ssl, allow_redirects=self.follow_redirects,
                         stream=True)

    def _scheme(self, url):
        return urlparse(url).scheme.lower()

    def validate_one(self, stream, health: bool = True) -> Result:
        """Validate a single stream. Returns Result (incl. health fields).

        `health=True` runs the full A+B health scoring (latency + throughput
        sampling). `health=False` is the cheap Phase-1 pass: only reachability
        is checked (HEAD/GET + latency), throughput sampling is skipped so dead
        links fail fast. Used by the Solution A 2-phase funnel.
        """
        res = Result()
        res.url = stream.original_url

        scheme = self._scheme(stream.original_url)
        if scheme in NON_HTTP_SCHEMES:
            res.uncheckable = True
            res.reason = f"non-http scheme: {scheme}"
            return res

        headers = _headers_for(stream, fallback_ua=self._ua_pool[hash(stream.url) % len(self._ua_pool)])

        # pick which URL to probe: tokened original_url (tokens preserved)
        url = stream.original_url
        is_tokened = any(
            p in (url.split("?", 1)[1] if "?" in url else "")
            for p in ("token", "sig", "signature", "sign", "token2", "auth",
                      "expires", "md5", "hmac", "nonce", "ts", "key", "hdnea")
        )

        last_err = ""
        r = None
        r2 = None
        try:
            for attempt in range(self.retries + 1):
                try:
                    t0 = time.monotonic()
                    r = self._do_request("HEAD", url, headers)
                    if r.status_code in (200, 206) and self._ct_ok(r):
                        res.ok = True
                        res.status = r.status_code
                        res.elapsed_ms = (time.monotonic() - t0) * 1000.0
                        self._measure_health(res, r, url, headers, health)
                        return res
                    # fall back to GET range
                    r2 = self._do_request("GET", url, headers)
                    if r2.status_code in (200, 206) and self._ct_ok(r2):
                        res.ok = True
                        res.status = r2.status_code
                        res.elapsed_ms = (time.monotonic() - t0) * 1000.0
                        self._measure_health(res, r2, url, headers, health)
                        return res
                    # 403/401 on a tokened URL -> suspected expired (C1)
                    if r.status_code in (401, 403) and is_tokened:
                        res.suspected_expired = True
                        res.reason = f"http_{r.status_code}_tokened"
                        return res
                    # 4xx (except 401/403 above) is deterministic -> do NOT retry
                    if 400 <= r.status_code < 500:
                        res.reason = f"http_{r.status_code}"
                        return res
                    last_err = f"http_{r.status_code}"
                except Exception as e:  # timeout/connection/ssl
                    last_err = type(e).__name__
                finally:
                    # release the (streamed) connections back to the pool
                    if r is not None:
                        try:
                            r.close()
                        except Exception:
                            pass
                        r = None
                    if r2 is not None:
                        try:
                            r2.close()
                        except Exception:
                            pass
                        r2 = None
                if attempt < self.retries:
                    time.sleep(self.backoff[min(attempt, len(self.backoff) - 1)])
        finally:
            if r is not None:
                try:
                    r.close()
                except Exception:
                    pass
            if r2 is not None:
                try:
                    r2.close()
                except Exception:
                    pass
        res.reason = last_err or "unknown"
        return res

    def _measure_health(self, res: Result, resp, url: str, headers: dict, health: bool = True):
        """Populate health_score / health_tier from latency (A) and optional
        throughput sampling (B). Bypassed entirely when its config is off.

        When `health=False` (Solution A Phase-1 cheap pass), only reachability
        was established; skip latency/throughput scoring so dead links fail fast
        and we don't waste a 3s throughput sample on a link we'll recheck in
        Phase 2 anyway. In that case health fields stay None.

        IMPORTANT: throughput sampling (B) only makes sense for actual media
        (segments). Playlist/manifest URLs (.m3u/.m3u8, HLS manifests) are tiny
        text files, so a 3s sample always measures near-zero KB/s and would
        falsely flag every channel as "slow". For those we rate by latency only.
        """
        if not health:
            # Phase-1: reachability already set res.ok; leave health unset.
            return
        # Is this a playlist/manifest URL? (no throughput sampling then)
        ct = (getattr(resp, "headers", {}).get("Content-Type", "") or "").lower()
        url_l = url.lower()
        is_manifest = (
            url_l.endswith(".m3u") or url_l.endswith(".m3u8")
            or "application/vnd.apple.mpegurl" in ct
            or "application/x-mpegurl" in ct
            or "application/mpegurl" in ct
            or url_l.endswith(".m3u?") or ".m3u8?" in url_l
        )

        # Option A: latency-based tier (cheap, always-on unless disabled)
        elapsed = res.elapsed_ms
        if self.latency_check:
            if elapsed <= self.healthy_max_ms:
                lat_tier = "healthy"
                lat_score = 100.0 - (elapsed / self.healthy_max_ms) * 30.0
            elif elapsed <= self.medium_max_ms:
                lat_tier = "medium"
                span = max(1.0, self.medium_max_ms - self.healthy_max_ms)
                lat_score = 70.0 - ((elapsed - self.healthy_max_ms) / span) * 30.0
            else:
                lat_tier = "slow"
                lat_score = max(10.0, 40.0 - (elapsed - self.medium_max_ms) / 100.0)
        else:
            lat_tier = "unknown"
            lat_score = 50.0

        # Option B: throughput sampling (accurate, costs traffic).
        # SKIP for playlist/manifest URLs — they are tiny text, not media.
        # Sampling is performed by _sample_throughput() which uses a FRESH
        # module-level requests.get (own connection, no shared pool) to avoid
        # exhausting the per-thread pooled session across 150 concurrent
        # workers (that deadlocked Phase 2).
        tp_tier = "unknown"
        tp_score = 50.0
        res.throughput_kbps = None  # meaningful only when actually sampled
        did_throughput = False
        if self.throughput_check and not self._client and not is_manifest:
            # Only sample with the real transport (tests use a fake client).
            did_throughput = True
            try:
                s_res, kbps = self._sample_throughput_raw(url, headers)
                res.throughput_kbps = kbps
                if kbps >= self.throughput_min_kbps:
                    tp_tier = "healthy"
                    tp_score = min(100.0, 60.0 + (kbps / self.throughput_min_kbps) * 20.0)
                else:
                    tp_tier = "slow"
                    tp_score = max(10.0, (kbps / self.throughput_min_kbps) * 50.0)
            except Exception:
                tp_tier = "unknown"
                tp_score = 50.0

        # Combine: if both enabled, weight latency 40% + throughput 60%
        # (throughput is the true buffer predictor). If one disabled, use other.
        # When throughput was skipped (manifest), treat its contribution as the
        # latency tier so we don't penalize playlists with a false "slow".
        if self.latency_check and self.throughput_check:
            if is_manifest:
                # manifest: latency alone is the health signal
                score, tier = lat_score, lat_tier
            else:
                # Throughput sample: use a FRESH module-level requests.get (its
                # own connection, no shared pool) rather than the pooled
                # thread-local session. Holding a pooled connection for the full
                # 3s sample across 150 concurrent workers exhausts the pool and
                # deadlocks the run (every worker blocks waiting for a free
                # connection). A one-shot get avoids pool contention entirely.
                score, tier = self._sample_throughput(
                    url, headers, lat_score, lat_tier
                )
        elif self.latency_check:

            score, tier = lat_score, lat_tier
        elif self.throughput_check:
            score, tier = tp_score, tp_tier
        else:
            score, tier = None, None
        res.health_score = round(score, 1) if score is not None else None
        res.health_tier = tier

    @staticmethod
    def _combine_tier(a: str, b: str) -> str:
        order = {"healthy": 0, "medium": 1, "slow": 2, "unknown": 1}
        if a == b:
            return a
        # worst of the two wins (slow dominates)
        return a if order.get(a, 1) >= order.get(b, 1) else b

    def _sample_throughput_raw(self, url: str, headers: dict):
        """Download for `throughput_sample_seconds` and return (resp, kbps).

        Uses a FRESH module-level ``requests.get`` (its own connection, no
        shared pool) so a 3s sample across 150 concurrent workers does NOT
        exhaust the per-thread pooled session and deadlock the run. A hard
        monotonic cap guarantees we never block longer than the sample window
        plus a small slack, even if the server trickles data.
        """
        import requests
        t0 = time.monotonic()
        nbytes = 0
        # one-shot get with its own connection; stream=True so we can stop early
        resp = requests.get(
            url, headers=headers, stream=True,
            timeout=(self.timeout[0], self.throughput_sample_seconds + 5),
            verify=self.verify_ssl, allow_redirects=self.follow_redirects,
        )
        try:
            for chunk in resp.iter_content(chunk_size=8192):
                nbytes += len(chunk)
                if (time.monotonic() - t0) >= self.throughput_sample_seconds:
                    break
        finally:
            resp.close()
        dt = time.monotonic() - t0
        kbps = (nbytes / 1024.0) / dt if dt > 0 else 0.0
        return resp, kbps

    def _ct_ok(self, resp) -> bool:
        """Content-Type gate (§4.1 / real-world tolerance).

        Reject only clear non-stream markers (text/html = error/login page).
        Accept everything else — many IPTV servers return text/plain, no
        Content-Type, or an opaque binary and still serve a working stream.
        A 2xx/3xx with any bytes is good enough for the player to try.
        """
        ct = (getattr(resp, "headers", {}).get("Content-Type", "") or "").lower()
        if not ct:
            return True
        if "text/html" in ct:
            return False
        return True

    def validate_batch(self, streams, progress=None, health: bool = True):
        """Validate a list of streams concurrently.

        If `isolate` is set (default True), the network I/O is performed in a
        separate child PROCESS so a native SEGV in the TLS/DNS stack (which
        Python cannot catch and which otherwise kills the whole pipeline) is
        contained: the child dies, the parent marks that batch failed, and the
        run continues. This is the crash-proof path.

        Returns a list of (stream, Result) — identical contract to the
        in-process path, so callers are unchanged.
        """
        # Isolation only helps contain native (C-level) crashes in the real TLS
        # stack. A custom http_client (test doubles, or any non-reconstructable
        # transport) cannot be passed to the child process, so it MUST run
        # in-process — otherwise the child rebuilds a fresh validator without
        # the injected client and validates against the real network.
        if getattr(self, "isolate", True) and self._client is None:
            try:
                return self._validate_batch_isolated(streams, progress, health)
            except Exception as e:  # noqa: BLE001
                _get_logger().exception("validate_batch isolation failed, "
                                        "falling back to in-process: %s", e)
                return self._validate_batch_inproc(streams, progress, health)
        return self._validate_batch_inproc(streams, progress, health)

    def _validate_batch_inproc(self, streams, progress=None, health: bool = True):
        """Validate a list of streams concurrently (thread pool). Returns
        a list of (stream, Result). `health=False` runs the cheap Phase-1 pass
        (reachability only, throughput skipped) for the Solution A funnel.

        Hang-safety design (critical):
        - Each link is bounded by a HARD wall-clock deadline (`hard_timeout`):
          a daemon thread runs validate_one; the pool worker joins with that
          timeout and abandons the link as a failure if it overruns. This
          catches DNS/TLS/trickle stalls the (connect,read) timeout misses.
        - The global network semaphore is acquired HERE, in the pool worker
          (which the join can abandon), NEVER inside the unkillable daemon
          thread. A native stall inside the daemon thread therefore can NOT
          hold the semaphore and exhaust it (which previously deadlocked the
          whole run at ~75% progress).
        - A process-global `socket.setdefaulttimeout` safety net is armed for
          the duration of the batch so even SSL-handshake native stalls are
          bounded (restored afterward)."""
        import threading as _threading
        import socket as _socket

        # Arm a global socket timeout safety net (last-resort against native
        # SSL-handshake / trickle stalls that urllib3 timeouts can miss).
        _prev_sock_to = _socket.getdefaulttimeout()
        _socket.setdefaulttimeout(self.hard_timeout + 5)
        _log = _get_logger()
        _log.info("validate_batch start n=%d workers=%d hard_timeout=%.0fs",
                  len(streams), self.workers, self.hard_timeout)

        def _run_one(s):
            # Acquire the global concurrency cap in the POOL WORKER (abandonable
            # via the join below), with a bounded wait so we can never block
            # forever if permits are (transiently) held by stalled threads.
            acquired = self._net_sem.acquire(timeout=self.hard_timeout)
            if not acquired:
                r = Result()
                r.url = s.original_url
                r.reason = "sem_timeout"
                return r
            try:
                box = {}

                def _target():
                    try:
                        box["r"] = self.validate_one(s, health)
                    except Exception as e:  # noqa: BLE001
                        box["e"] = e

                t = _threading.Thread(target=_target, daemon=True)
                t.start()
                t.join(timeout=self.hard_timeout)
                if t.is_alive():
                    r = Result()
                    r.url = s.original_url
                    r.reason = f"hard_timeout_{int(self.hard_timeout)}s"
                    return r
                if "e" in box:
                    r = Result()
                    r.url = s.original_url
                    r.reason = f"exc:{type(box['e']).__name__}"
                    return r
                return box.get("r") or Result()
            finally:
                self._net_sem.release()

        results = []
        done = 0
        n = len(streams)
        _t0 = _time.monotonic()
        try:
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                fut_map = {ex.submit(_run_one, s): s for s in streams}
                for fut in as_completed(fut_map):
                    s = fut_map[fut]
                    try:
                        r = fut.result()
                    except Exception as e:
                        r = Result()
                        r.url = s.original_url
                        r.reason = f"exc:{type(e).__name__}"
                    results.append((s, r))
                    done += 1
                    if progress:
                        progress(done, n)
                    if done % 1000 == 0:
                        _log.info("validate_batch progress %d/%d (%.1fs)",
                                  done, n, _time.monotonic() - _t0)
        finally:
            _socket.setdefaulttimeout(_prev_sock_to)
            _log.info("validate_batch done n=%d in %.1fs", len(results),
                      _time.monotonic() - _t0)
        return results

    # ------------------------------------------------------------------
    # Subprocess-isolated validation (crash-proof against native SEGV)
    # ------------------------------------------------------------------
    def _validate_batch_isolated(self, streams, progress=None, health: bool = True):
        """Run validation in child processes so a native TLS/DNS SEGV is
        contained. Splits the work into chunks; each chunk is validated by a
        separate `multiprocessing` child that writes a results JSON. A child
        that crashes (SEGV) or exceeds the wall-clock budget is terminated and
        its streams are recorded as failures — the run never dies."""
        import multiprocessing as _mp
        import tempfile as _tf

        # Use 'spawn' (not the Linux default 'fork'): forked children inherit
        # the parent's module-level ThreadPoolExecutors (DNS pool) with NO
        # worker threads, which makes pooled submissions silently stall and
        # inflate per-link time far beyond hard_timeout. spawn starts a clean
        # interpreter so the child re-imports and builds fresh pools.
        try:
            _mp.set_start_method("spawn", force=True)
        except Exception:
            pass

        _log = _get_logger()
        n = len(streams)
        if n == 0:
            return []
        chunk = max(1, int(self.config.get("validation.isolate_chunk", 200)))
        chunks = [streams[i:i + chunk] for i in range(0, n, chunk)]

        def _ser(s):
            return {
                "id": getattr(s, "id", None),
                "url": getattr(s, "url", ""),
                "original_url": getattr(s, "original_url", ""),
                "attributes": getattr(s, "attributes", {}) or {},
            }

        _log.info("validate_batch_isolated start n=%d chunks=%d chunk=%d workers=%d",
                  n, len(chunks), chunk, self.workers)
        _t0 = _time.monotonic()
        # self.config is a Config dataclass; pass its underlying dict to the
        # child and let the child reconstruct a Config (dotted .get support).
        cfg = dict(self.config.data)
        cfg.pop("logging", None)
        config_dir = getattr(self.config, "config_dir", "")
        config_path = getattr(self.config, "config_path", "")

        workdir = _tf.mkdtemp(prefix="m3u_validate_")
        out_dir = _tf.mkdtemp(prefix="m3u_results_")
        chunk_paths = []
        for i, ch in enumerate(chunks):
            p = os.path.join(workdir, f"chunk_{i}.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump([_ser(s) for s in ch], f)
            chunk_paths.append(p)

        procs = []
        for i, p in enumerate(chunk_paths):
            rpath = os.path.join(out_dir, f"res_{i}.json")
            pr = _mp.Process(
                target=_isolated_worker,
                args=(cfg, config_dir, config_path, p, rpath, health, self.workers,
                      self.hard_timeout, self.max_concurrent, self.retries),
            )
            pr.start()
            procs.append((i, pr, rpath, chunks[i]))

        # Collect chunks in PARALLEL: each subprocess validates concurrently,
        # so total wall-clock ≈ the worst single chunk (not the sum). A
        # per-chunk budget caps a runaway; a crashed/dead chunk is marked
        # failed and the rest continue.
        results = []
        done = 0
        n_chunks = len(procs)

        def _collect_one(item):
            i, pr, rpath, ch = item
            budget = self.hard_timeout * max(1, (len(ch) // self.max_concurrent)) + 120
            pr.join(timeout=budget)
            status = "ok"
            if pr.is_alive():
                _log.error("validate chunk %d exceeded budget (%.0fs) -> terminate",
                           i, budget)
                pr.terminate()
                try:
                    pr.join(timeout=10)
                except Exception:
                    pass
                self._append_chunk_failures(results, ch)
                status = "timeout"
            elif pr.exitcode is not None and pr.exitcode < 0:
                _log.error("validate chunk %d child died (exitcode=%s) -> failures",
                           i, pr.exitcode)
                self._append_chunk_failures(results, ch)
                status = "crash"
            else:
                self._read_chunk_results(results, ch, rpath)
            return (i, len(ch), status)

        with ThreadPoolExecutor(max_workers=max(1, min(n_chunks, 8))) as pool:
            for i, n_ch, status in pool.map(_collect_one, procs):
                done += n_ch
                if progress:
                    progress(done, n)
        try:
            shutil.rmtree(workdir, ignore_errors=True)
            shutil.rmtree(out_dir, ignore_errors=True)
        except Exception:
            pass
        _log.info("validate_batch_isolated done n=%d in %.1fs", len(results), _time.monotonic() - _t0)
        return results

    @staticmethod
    def _append_chunk_failures(results, ch):
        for s in ch:
            r = Result()
            r.url = getattr(s, "original_url", "")
            r.reason = "subprocess_crash"
            results.append((s, r))

    @staticmethod
    def _read_chunk_results(results, ch, rpath):
        try:
            with open(rpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            StreamValidator._scrub_failures(results, ch)
            return
        by_id = {getattr(s, "id", None): s for s in ch}
        for d in data:
            s = by_id.get(d.get("id"))
            if s is None:
                continue
            r = Result()
            r.url = d.get("url", "")
            r.ok = bool(d.get("ok"))
            r.status = int(d.get("status", 0) or 0)
            r.reason = d.get("reason", "") or ""
            r.uncheckable = bool(d.get("uncheckable"))
            r.suspected_expired = bool(d.get("suspected_expired"))
            r.elapsed_ms = float(d.get("elapsed_ms", 0.0) or 0.0)
            r.throughput_kbps = float(d.get("throughput_kbps", 0.0) or 0.0)
            r.health_score = d.get("health_score")
            r.health_tier = d.get("health_tier")
            results.append((s, r))

    @staticmethod
    def _scrub_failures(results, ch):
        for s in ch:
            r = Result()
            r.url = getattr(s, "original_url", "")
            r.reason = "subprocess_crash"
            results.append((s, r))


def _isolated_worker(cfg, config_dir, config_path, chunk_path, result_path, health,
                     workers, hard_timeout, max_concurrent, retries):
    """Child-process entry point: validate one chunk, write results JSON."""
    import faulthandler
    faulthandler.enable()
    try:
        from .logging_utils import configure_logging
        _lcfg = (cfg or {}).get("logging", {}) or {}
        configure_logging(
            level=_lcfg.get("level", "WARNING"),
            log_file=_lcfg.get("file"),
            json_format=bool(_lcfg.get("json_format", False)),
            log_write=bool(_lcfg.get("log_write", True)),
        )
    except Exception:
        pass
    try:
        # reconstruct a Config (StreamValidator uses dotted .get)
        from .config import Config
        conf = Config(_data=cfg, config_dir=config_dir or "", config_path=config_path or "")
        with open(chunk_path, "r", encoding="utf-8") as f:
            items = json.load(f)
        v = StreamValidator(conf, workers=workers, retries=retries)
        v.hard_timeout = hard_timeout
        v.max_concurrent = max_concurrent
        v.isolate = False

        class _S:
            def __init__(self, d):
                self.id = d.get("id")
                self.url = d.get("url", "")
                self.original_url = d.get("original_url", "")
                self.attributes = d.get("attributes", {}) or {}

        streams = [_S(d) for d in items]
        import time as _tmod
        from concurrent.futures import ThreadPoolExecutor as _TPE
        _chunk_t0 = _tmod.monotonic()

        def _one(s):
            try:
                return s, v.validate_one(s, health)
            except Exception as e:  # noqa: BLE001
                r = Result()
                r.reason = f"exc:{type(e).__name__}"
                r.url = s.original_url
                return s, r

        out = []
        # The child is ALREADY an isolated process (a SEGV only kills this
        # chunk); run validate_one concurrently with a real pool. No nested
        # daemon-thread wrapper needed — the subprocess boundary contains SEGVs,
        # and request timeouts bound each link.
        with _TPE(max_workers=max(1, workers)) as ex:
            for s, res in ex.map(_one, streams):
                out.append({
                    "id": s.id,
                    "url": res.url,
                    "ok": res.ok,
                    "status": res.status,
                    "reason": res.reason,
                    "uncheckable": res.uncheckable,
                    "suspected_expired": res.suspected_expired,
                    "elapsed_ms": res.elapsed_ms,
                    "throughput_kbps": res.throughput_kbps,
                    "health_score": res.health_score,
                    "health_tier": res.health_tier,
                })
        _get_logger().info("isolated chunk validated n=%d in %.1fs",
                           len(out), _tmod.monotonic() - _chunk_t0)
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(out, f)
        # Hard exit: this child has no further work and any daemon threads
        # still blocked in native TLS/DNS calls would otherwise stall
        # interpreter shutdown and keep the parent's join() waiting the full
        # budget. Results are already persisted, so _exit is safe.
        os._exit(0)
    except Exception:
        os._exit(1)

