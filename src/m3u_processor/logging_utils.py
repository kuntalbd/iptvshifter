"""Shared logging setup for iptvshifter.

All modules should call `from .logging_utils import get_logger` and use the
returned logger instead of `print()` so that log level / file routing is
consistent and observable. The web service and CLI both configure the root
logger via `configure_logging()`.

Config keys honored (config.yaml ``logging:`` section):
  level        DEBUG/INFO/WARNING/ERROR  (root threshold)
  log_write    False -> attach no handler at all (silent)
  file         target log file (default stderr)
  json_format  True -> pythonjsonlogger JSON formatter (falls back to plain)
  max_bytes    rotation size for the file handler (0/unset -> no rotation)
  backup_count  number of rotated files kept
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import sys

_CONFIGURED = False


def get_logger(name: str = "m3u") -> logging.Logger:
    """Return a module logger; lazily ensures basic config exists."""
    if not _CONFIGURED:
        configure_logging()
    return logging.getLogger(name)


def configure_logging(level: str | None = None, log_file: str | None = None,
                       json_format: bool = False, log_write: bool = True,
                       max_bytes: int | None = None,
                       backup_count: int | None = None) -> None:
    """Configure the root logger once.

    level: DEBUG/INFO/WARNING/ERROR (default INFO).
    log_file: optional path; if omitted, logs to stderr.
    log_write: if False, NO file/stream handler is attached — logging is
        suppressed (config-driven, e.g. logging.log_write: false). Modules that
        already captured a reference to the root logger keep working, they just
        emit into the void.
    max_bytes: rotation size for a RotatingFileHandler when log_file is set.
        <=0 / None -> plain FileHandler (no rotation).
    backup_count: number of rotated files retained (default 5).
    """
    global _CONFIGURED
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)

    root = logging.getLogger()
    # avoid attaching handlers twice
    for h in list(root.handlers):
        root.removeHandler(h)

    # log_write: false => do not attach any handler (no file, no stderr).
    if log_write:
        handler: logging.Handler
        if log_file:
            try:
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                if max_bytes and int(max_bytes) > 0:
                    handler = logging.handlers.RotatingFileHandler(
                        log_file,
                        maxBytes=int(max_bytes),
                        backupCount=int(backup_count or 5),
                        encoding="utf-8",
                    )
                else:
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
