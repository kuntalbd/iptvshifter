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
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

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
        self.max_concurrent = int(config.get("validation.max_concurrent", 40))
        # Hard wall-clock cap per link. The (connect,read) request timeout resets
        # on every successful byte, so a server that trickles 1 byte / few-seconds
        # NEVER trips it and a handful of such links stall as_completed forever
        # (the whole run hangs). A per-link daemon-thread deadline caps total time
        # no matter WHERE it stalls (DNS, TLS handshake, or slow-trickle body).
        self.hard_timeout = float(config.get("validation.hard_timeout", 20))
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
            s.mount("https://", HTTPAdapter(pool_connections=20, pool_maxsize=50))
            s.mount("http://", HTTPAdapter(pool_connections=20, pool_maxsize=50))
            self._thread_local.session = s
            local = s
        return local

    def _do_request(self, method, url, headers):
        # Cap concurrent in-flight network calls so we never overwhelm the DNS
        # resolver / TLS stack with workers=150 simultaneous handshakes (which
        # deadlocked the run). The semaphore serializes the actual I/O; extra
        # worker threads queue for a slot.
        # stream=True: we only need status + headers (reachability / Content-Type).
        # Without it, requests buffers the ENTIRE response body, and a large or
        # slow m3u8 playlist can hang the read far past the (connect,read) timeout
        # -> threads pile up stuck in ssl.read and as_completed never completes
        # -> the whole run deadlocks. Callers close() the response when done.
        with self._net_sem:
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
        """Validate a list of streams concurrently (thread pool). Returns
        a list of (stream, Result). `health=False` runs the cheap Phase-1 pass
        (reachability only, throughput skipped) for the Solution A funnel.

        Each link is bounded by a HARD wall-clock deadline (`hard_timeout`): if a
        single validate_one exceeds it (DNS/TLS/trickle stall the request timeout
        can't catch), the link is abandoned as a failure so the batch always
        terminates. The ThreadPoolExecutor workers also acquire the global
        network semaphore so we never fire unbounded concurrent DNS/TLS."""
        import threading as _threading

        def _run_one(s):
            # hard deadline wrapper
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
                # exceeded the hard cap (trickle/TLS stall) -> treat as failure
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

        results = []
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            fut_map = {ex.submit(_run_one, s): s for s in streams}
            done = 0
            for fut in as_completed(fut_map):
                s = fut_map[fut]
                try:
                    r = fut.result()
                except Exception as e:
                    r = Result(); r.url = s.original_url; r.reason = f"exc:{type(e).__name__}"
                results.append((s, r))
                done += 1
                if progress:
                    progress(done, len(streams))
        return results
