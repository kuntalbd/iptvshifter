"""Shared logging setup for iptvshifter.

All modules should call `from .logging_utils import get_logger` and use the
returned logger instead of `print()` so that log level / file routing is
consistent and observable. The web service and CLI both configure the root
logger via `configure_logging()`.
"""
from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False


def get_logger(name: str = "m3u") -> logging.Logger:
    """Return a module logger; lazily ensures basic config exists."""
    if not _CONFIGURED:
        configure_logging()
    return logging.getLogger(name)


def configure_logging(level: str | None = None, log_file: str | None = None,
                       json_format: bool = False) -> None:
    """Configure the root logger once.

    level: DEBUG/INFO/WARNING (default INFO).
    log_file: optional path; if omitted, logs to stderr.
    """
    global _CONFIGURED
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)

    root = logging.getLogger()
    # avoid attaching handlers twice
    for h in list(root.handlers):
        root.removeHandler(h)

    handler: logging.Handler
    if log_file:
        try:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            handler = logging.FileHandler(log_file, encoding="utf-8")
        except OSError:
            handler = logging.StreamHandler(sys.stderr)
    else:
        handler = logging.StreamHandler(sys.stderr)

    if json_format:
        try:
            from pythonjsonlogger import jsonlogger  # type: ignore

            handler.setFormatter(
                jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            )
        except Exception:
            handler.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"))
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"))

    root.addHandler(handler)
    root.setLevel(lvl)
    _CONFIGURED = True
