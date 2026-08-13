#!/usr/bin/env python3
"""Ensure the moomoo OpenD daemon is listening on 127.0.0.1:11111 before the bridge runs.

Why this exists: RefreshData fires at 06:00, but the "moomoo OpenD" scheduled
task is LOGON-triggered — on any morning nobody is logged in before 6 AM the
daemon is down, and opend_bridge.py silently degrades (options greeks / ETF AUM /
expected-move / S&P-500 sector weights all fall back to yfinance, which then
rate-limits). Observed 2026-08-13: both 06:00/06:30 builds ran while OpenD was
down (daemon didn't come up until logon at ~09:00), leaving the heatmap weight
column 100% null.

Contract: exit 0 = daemon reachable, exit 1 = not up within timeout. The bat
treats exit 1 as WARN and lets the bridge fall back exactly as it does today, so
the scheduled build never hangs on a stuck or self-updating OpenD.
"""
import argparse
import socket
import subprocess
import sys
import time

HOST, PORT = "127.0.0.1", 11111
DEFAULT_EXE = r"C:\Users\zhenyuyong\AppData\Roaming\moomoo_OpenD\moomoo_OpenD.exe"


def reachable(timeout=2.0):
    s = socket.socket()
    s.settimeout(timeout)
    try:
        return s.connect_ex((HOST, PORT)) == 0
    finally:
        s.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--exe", default=DEFAULT_EXE, help="path to moomoo_OpenD.exe")
    ap.add_argument("--timeout", type=int, default=120,
                    help="seconds to wait for the daemon after launch")
    args = ap.parse_args()

    if reachable():
        print("[opend] daemon already running on %s:%d" % (HOST, PORT))
        return 0

    print("[opend] daemon not up — launching %s" % args.exe)
    try:
        # Detached so it outlives this build process, like the scheduled task does.
        subprocess.Popen(
            [args.exe],
            creationflags=getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            close_fds=True,
        )
    except FileNotFoundError:
        print("[opend] ERROR: executable not found: %s" % args.exe, file=sys.stderr)
        return 1
    except Exception as e:  # noqa: BLE001 - report and fall back, never hang the build
        print("[opend] ERROR launching: %s" % e, file=sys.stderr)
        return 1

    print("[opend] waiting up to %ds for %s:%d ..." % (args.timeout, HOST, PORT))
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        time.sleep(3)
        if reachable():
            waited = args.timeout - int(deadline - time.monotonic())
            print("[opend] daemon reachable after ~%ds" % waited)
            return 0
    print("[opend] WARN: not reachable within %ds — bridge will fall back to yfinance/baseline"
          % args.timeout, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
