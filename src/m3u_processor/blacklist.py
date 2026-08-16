"""Blacklist state machine + escalation + purge (§5, §18.3, §20.3, F26).

Transition table implemented as `apply_result(stream, ok, suspected_expired)`.
Also handles the edge-case F26 (short + last_working IS NULL relies on
total_failures threshold).
"""
from __future__ import annotations

from datetime import datetime, timezone


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
        stream.consecutive_failures = 0
        stream.total_successes += 1
        stream.last_working = _now_iso()
        stream.is_working = True
        return transition

    # failure path
    stream.is_working = False
    stream.consecutive_failures += 1
    stream.total_failures += 1
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
        return transition

    # short threshold
    if stream.blacklist_tier == "none" and stream.consecutive_failures >= short_th:
        transition["event"] = "short_added"
        transition["new_tier"] = "short"
        stream.blacklist_tier = "short"
        stream.blacklisted_at = _now_iso()
        stream.blacklist_reason = f"{stream.consecutive_failures} consecutive fails"
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
                return transition
        except Exception:
            pass

    transition["event"] = "failure_counted"
    transition["new_tier"] = stream.blacklist_tier
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
    return escalated


def purge_old(db, cfg, run_id=""):
    """Remove streams not checked in `purge_unchecked_days` (§5.3)."""
    days = int(cfg.get("blacklist.purge_unchecked_days", 90))
    db.execute(
        "DELETE FROM streams WHERE last_checked IS NULL AND first_seen < "
        "datetime('now', ?)",
        (f"-{days} days",),
    )
    db.execute(
        "DELETE FROM streams WHERE last_checked IS NOT NULL AND last_checked < "
        "datetime('now', ?)",
        (f"-{days} days",),
    )
    db.commit()
