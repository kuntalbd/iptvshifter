"""Blacklist state machine + escalation + purge (§5, §18.3, §20.3, F26).

Transition table implemented as `apply_result(stream, ok, suspected_expired)`.
Also handles the edge-case F26 (short + last_working IS NULL relies on
total_failures threshold).
"""
from __future__ import annotations

from datetime import datetime, timezone

from .logging_utils import get_logger as _get_logger

_LOG = _get_logger("m3u.blacklist")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def apply_result(stream, ok: bool, suspected_expired: bool, cfg, run_id="",
                 triggered_by="validator"):
    """Mutate stream counters/tier based on a validation outcome.

    Returns dict describing the transition (for events/logging).
    """
    short_th = int(cfg.get("blacklist.short_threshold", 3))
    perm_fail = int(cfg.get("blacklist.permanent_failure_threshold", 10))
    perm_days = int(cfg.get("blacklist.permanent_inactive_days", 30))
    escalate = bool(cfg.get("blacklist.escalate_enabled", True))

    old_tier = stream.blacklist_tier
    transition = {"stream_id": stream.id, "old_tier": old_tier,
                  "new_tier": old_tier, "event": None, "reason": ""}

    if ok:
        if stream.blacklist_tier in ("short", "permanent"):
            transition["event"] = ("recovered" if stream.blacklist_tier == "short"
                                   else "recovered_from_permanent")
            transition["new_tier"] = "none"
            stream.blacklist_tier = "none"
            stream.blacklisted_at = None
            stream.blacklist_reason = ""
            _LOG.info("blacklist recovery stream_id=%s event=%s",
                      stream.id, transition["event"])
        stream.consecutive_failures = 0
        stream.consecutive_pass += 1
        stream.total_pass += 1
        stream.total_successes += 1
        stream.last_working = _now_iso()
        stream.is_working = True
        return transition

    # failure path
    stream.is_working = False
    stream.consecutive_failures += 1
    stream.total_failures += 1
    stream.consecutive_pass = 0
    stream.last_checked = _now_iso()

    # escalation: short + inactive long enough -> permanent
    if escalate and stream.blacklist_tier == "short" and stream.last_working:
        try:
            lw = datetime.fromisoformat(stream.last_working)
            days = (datetime.now(timezone.utc) - lw).days
            if days >= perm_days:
                transition["event"] = "escalated"
                transition["new_tier"] = "permanent"
                stream.blacklist_tier = "permanent"
                stream.blacklisted_at = _now_iso()
                stream.blacklist_reason = f"inactive {days}d"
                _LOG.warning("blacklist escalated stream_id=%s inactive=%dd",
                             stream.id, days)
                return transition
        except Exception:
            pass

    # never worked -> permanent after total_failures threshold (F26)
    if (stream.last_working is None
            and stream.total_failures >= perm_fail
            and stream.blacklist_tier != "permanent"):
        transition["event"] = "permanent_added"
        transition["new_tier"] = "permanent"
        stream.blacklist_tier = "permanent"
        stream.blacklisted_at = _now_iso()
        stream.blacklist_reason = f"never worked, {stream.total_failures} fails"
        _LOG.warning("blacklist permanent_added stream_id=%s fails=%d",
                     stream.id, stream.total_failures)
        return transition

    # short threshold
    if stream.blacklist_tier == "none" and stream.consecutive_failures >= short_th:
        transition["event"] = "short_added"
        transition["new_tier"] = "short"
        stream.blacklist_tier = "short"
        stream.blacklisted_at = _now_iso()
        stream.blacklist_reason = f"{stream.consecutive_failures} consecutive fails"
        _LOG.info("blacklist short_added stream_id=%s fails=%d",
                  stream.id, stream.consecutive_failures)
        return transition

    # permanent via inactive days even if last_working set (direct)
    if escalate and stream.blacklist_tier == "none" and stream.last_working:
        try:
            lw = datetime.fromisoformat(stream.last_working)
            days = (datetime.now(timezone.utc) - lw).days
            if days >= perm_days:
                transition["event"] = "permanent_added"
                transition["new_tier"] = "permanent"
                stream.blacklist_tier = "permanent"
                stream.blacklisted_at = _now_iso()
                stream.blacklist_reason = f"inactive {days}d"
                _LOG.warning("blacklist permanent_added stream_id=%s inactive=%dd",
                             stream.id, days)
                return transition
        except Exception:
            pass

    transition["event"] = "failure_counted"
    transition["new_tier"] = stream.blacklist_tier
    _LOG.debug("blacklist stream_id=%s old=%s new=%s fails=%d event=%s",
               stream.id, old_tier, stream.blacklist_tier,
               stream.total_failures, transition["event"])
    return transition


def escalate_short_to_permanent(db, cfg, run_id=""):
    """Bulk escalate short->permanent for streams inactive beyond threshold."""
    perm_days = int(cfg.get("blacklist.permanent_inactive_days", 30))
    rows = db.query(
        "SELECT id, last_working, blacklist_tier FROM streams WHERE blacklist_tier='short'"
    )
    now = datetime.now(timezone.utc)
    escalated = 0
    for r in rows:
        if not r["last_working"]:
            continue
        try:
            lw = datetime.fromisoformat(r["last_working"])
            if (now - lw).days >= perm_days:
                db.execute(
                    "UPDATE streams SET blacklist_tier='permanent', blacklisted_at=?, "
                    "blacklist_reason=? WHERE id=?",
                    (_now_iso(), f"inactive {(now-lw).days}d", r["id"]),
                )
                escalated += 1
        except Exception:
            pass
    db.commit()
    _LOG.info("escalate_short_to_permanent escalated=%d", escalated)
    return escalated


def purge_old(db, cfg, run_id=""):
    """Remove streams not checked in `purge_unchecked_days` (§5.3).

    Returns the number of streams deleted. Skipped entirely when the threshold
    is <= 0 (operator opted out).
    """
    days = int(cfg.get("blacklist.purge_unchecked_days", 90))
    if days <= 0:
        _LOG.info("purge_old skipped (purge_unchecked_days=%s)", days)
        return 0
    cur = db.execute(
        "DELETE FROM streams WHERE last_checked IS NULL AND first_seen < "
        "datetime('now', ?)",
        (f"-{days} days",),
    )
    removed = cur.rowcount
    cur2 = db.execute(
        "DELETE FROM streams WHERE last_checked IS NOT NULL AND last_checked < "
        "datetime('now', ?)",
        (f"-{days} days",),
    )
    removed += cur2.rowcount
    db.commit()
    _LOG.info("purge_old removed %d streams not checked in %dd", removed, days)
    return removed
