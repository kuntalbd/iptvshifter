"""Provider (domain) enable/disable + auto-discovery (§6.2, §6.3, §13)."""
from __future__ import annotations

from .utils import extract_domain


def ensure_provider(db, domain: str, aggregate_subdomains: bool = True):
    """Auto-create a provider row on first sight (§6.3). Returns enabled flag."""
    from .utils import extract_domain
    dom = extract_domain(f"https://{domain}/", aggregate_subdomains)
    row = db.query("SELECT enabled FROM providers WHERE domain=?", (dom,))
    if not row:
        db.execute(
            "INSERT OR IGNORE INTO providers(domain, enabled, first_seen, updated_at) "
            "VALUES(?,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            (dom,),
        )
        db.commit()
        return True
    return bool(row[0]["enabled"])


def set_provider_enabled(db, domain: str, enabled: bool, reason="", by=""):
    db.execute(
        "INSERT INTO providers(domain, enabled, disabled_at, disabled_reason, updated_at) "
        "VALUES(?,?,?,?,CURRENT_TIMESTAMP) "
        "ON CONFLICT(domain) DO UPDATE SET enabled=excluded.enabled, "
        "disabled_at=excluded.disabled_at, disabled_reason=excluded.disabled_reason, "
        "updated_at=CURRENT_TIMESTAMP",
        (domain, 1 if enabled else 0,
         None if enabled else _now(), reason),
    )
    db.execute(
        "INSERT INTO enable_events(domain, event_type, reason, triggered_by) VALUES(?,?,?,?)",
        (domain, "provider_enabled" if enabled else "provider_disabled", reason, by),
    )
    db.commit()


def provider_enabled(db, stream) -> bool:
    """Resolve whether a stream's provider is enabled (§6.2)."""
    dom = stream.provider_domain
    row = db.query("SELECT enabled FROM providers WHERE domain=?", (dom,))
    if not row:
        return True  # not yet discovered -> treat enabled
    return bool(row[0]["enabled"])


def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
