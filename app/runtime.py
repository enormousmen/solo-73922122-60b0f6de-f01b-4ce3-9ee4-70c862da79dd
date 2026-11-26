"""Process runtime helpers."""

from __future__ import annotations

import os
import sys
import threading


def install_stdin_eof_watchdog() -> None:
    """Self-terminate when stdin reaches EOF.

    Opt-in via ``EXIT_ON_STDIN_EOF=1``.  The end-to-end self-test uses it
    because some sandboxes forbid sending signals to child processes.  It is
    never enabled by the Docker images / compose file.
    """
    if os.environ.get("EXIT_ON_STDIN_EOF") != "1":
        return

    def _watch() -> None:
        try:
            sys.stdin.buffer.read()
        except Exception:
            pass
        # Exit immediately once the parent closes the pipe.
        os._exit(0)

    threading.Thread(target=_watch, name="stdin-eof-watchdog", daemon=True).start()
