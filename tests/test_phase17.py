"""Phase 17 — config-driven logging (log_write / log_level).

Verifies logging_utils honors the config `logging.log_write` and
`logging.level` settings, so an operator can disable file logging or
raise/lower verbosity without code changes.
"""
import logging
import os
import tempfile

from m3u_processor.logging_utils import configure_logging, get_logger


def test_log_write_false_suppresses_handlers():
    """log_write:false must attach NO handler (no file, no stderr)."""
    path = tempfile.mktemp(suffix=".log")
    try:
        configure_logging(level="DEBUG", log_file=path, log_write=False)
        assert len(logging.getLogger().handlers) == 0, "handlers should be 0 when log_write=false"
        get_logger("t17a").info("should not be written")
        assert not os.path.exists(path), "no log file should be created when log_write=false"
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_log_level_filters_messages():
    """level:WARNING must hide DEBUG/INFO but show WARNING+."""
    path = tempfile.mktemp(suffix=".log")
    try:
        configure_logging(level="WARNING", log_file=path, log_write=True)
        log = get_logger("t17b")
        log.debug("hidden-debug")
        log.info("hidden-info")
        log.warning("shown-warning")
        with open(path) as f:
            content = f.read()
        assert "hidden-debug" not in content
        assert "hidden-info" not in content
        assert "shown-warning" in content
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_log_level_debug_shows_debug():
    """level:DEBUG must surface DEBUG messages."""
    path = tempfile.mktemp(suffix=".log")
    try:
        configure_logging(level="DEBUG", log_file=path, log_write=True)
        get_logger("t17c").debug("visible-debug")
        with open(path) as f:
            assert "visible-debug" in f.read()
    finally:
        if os.path.exists(path):
            os.remove(path)
