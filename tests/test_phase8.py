"""Phase 8 tests: deployment unit generation + hardening directives (§22)."""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor.deploy import generate, cron_to_oncalendar


def test_generate_units_exist_and_hardened():
    out = tempfile.mkdtemp()
    jobs = [
        {"name": "daily-full", "mode": "full", "cron": "0 2 * * *"},
        {"name": "token-refresh", "mode": "refresh", "cron": "7 */2 * * *"},
    ]
    files = generate(user="m3u", workdir="/opt/m3u-processor",
                     config="/opt/m3u-processor/config.yaml", port=8080,
                     jobs=jobs, outdir=out)
    # 2 run jobs -> 2 service + 2 timer + 1 web = 5
    assert "m3u-processor-daily-full.service" in files
    assert "m3u-processor-daily-full.timer" in files
    assert "m3u-processor-token-refresh.service" in files
    assert "m3u-processor-web.service" in files

    svc = open(os.path.join(out, "m3u-processor-daily-full.service")).read()
    # §22 Pi hardening
    assert "MemoryMax=400M" in svc
    assert "Nice=10" in svc
    assert "NoNewPrivileges=true" in svc
    assert "ProtectSystem=strict" in svc
    # correct exec uses --job (not --mode) and resolves mode from config.
    # --config must come BEFORE the subcommand (argparse requirement).
    assert "--job daily-full" in svc
    assert "m3u_processor --config" in svc and "run --job daily-full" in svc
    # restart=no for oneshot run
    assert "Restart=no" in svc

    web = open(os.path.join(out, "m3u-processor-web.service")).read()
    assert "m3u_processor --config" in web and "serve" in web
    assert "Restart=on-failure" in web
    assert "MemoryMax=400M" in web

    timer = open(os.path.join(out, "m3u-processor-daily-full.timer")).read()
    assert "OnCalendar=" in timer
    assert "WantedBy=timers.target" in timer
    assert "--job" not in timer  # timer only triggers, service carries the job


def test_fresh_eye_full_mode_unit():
    out = tempfile.mkdtemp()
    jobs = [{"name": "x", "mode": "full", "cron": "0 2 * * *"}]
    files = generate(jobs=jobs, outdir=out)
    svc = open(os.path.join(out, "m3u-processor-x.service")).read()
    assert "--job x" in svc
    tmr = open(os.path.join(out, "m3u-processor-x.timer")).read()
    assert "OnCalendar=*-*-* 02:00:00" in tmr


def test_cron_conversion_valid():
    # all conversions must be parseable by systemd (verified live via analyze)
    for cron in ["0 2 * * *", "7 */2 * * *", "0 3 * * FRI"]:
        oc = cron_to_oncalendar(cron)
        assert "OnCalendar" not in oc  # it's the value, not the key


def test_fresh_eye_config_before_subcommand():
    # F-39 regression: --config must precede the subcommand, else argparse
    # rejects it and the systemd unit fails at runtime.
    out = tempfile.mkdtemp()
    jobs = [{"name": "tok", "mode": "refresh", "cron": "7 */2 * * *"}]
    files = generate(jobs=jobs, outdir=out, service_target="default.target")
    svc = open(os.path.join(out, "m3u-processor-tok.service")).read()
    # correct: --config before run
    assert "--config" in svc and svc.index("--config") < svc.index("run --job")
    assert "run --job tok" in svc
    # web service also correct
    web = open(os.path.join(out, "m3u-processor-web.service")).read()
    assert "--config" in web and web.index("--config") < web.index("serve")
    # user-scope binds to default.target
    assert "WantedBy=default.target" in svc


if __name__ == "__main__":
    import traceback
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"FAIL {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
