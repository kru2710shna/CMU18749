#!/usr/bin/env python3
"""Launch one component in a separate terminal and wait for it to exit.

Milestone 5 LFDs execute their recovery command directly and track that
process to make retries idempotent. This helper preserves that property while
putting a recovered server's output somewhere separate from the LFD output:

* ``windows`` creates a new Windows console and waits for the child.
* ``tmux`` creates a new window in an existing session and waits for its pane
  to disappear.

The helper is demo tooling only. It does not participate in membership,
failure detection, elections, state transfer, or recovery decisions.
"""

import argparse
import os
import shlex
import subprocess
import sys
import time


def run_windows(command: list) -> int:
    if os.name != "nt":
        raise SystemExit("The windows terminal backend requires Windows.")

    child = subprocess.Popen(
        command,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )
    try:
        return child.wait()
    except KeyboardInterrupt:
        child.terminate()
        return child.wait()


def run_tmux(session: str, title: str, command: list) -> int:
    if not session:
        raise SystemExit("--tmux-session is required for the tmux backend.")

    # tmux accepts its pane command as one shell string. shlex.join quotes each
    # argv element, including paths with spaces, before tmux's shell sees it.
    shell_command = shlex.join(command)
    result = subprocess.run(
        [
            "tmux",
            "new-window",
            "-d",
            "-P",
            "-F",
            "#{pane_id}",
            "-t",
            session,
            "-n",
            title,
            shell_command,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    pane_id = result.stdout.strip()

    # The LFD tracks this helper. Keep it alive exactly as long as the pane's
    # server process, so an RM retry cannot create a duplicate server.
    while True:
        pane = subprocess.run(
            [
                "tmux",
                "display-message",
                "-p",
                "-t",
                pane_id,
                "#{pane_id}",
            ],
            capture_output=True,
            text=True,
        )
        if pane.returncode != 0:
            return 0
        time.sleep(0.25)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch a recovered component in a separate terminal."
    )
    parser.add_argument("--backend", choices=["tmux", "windows"], required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--tmux-session", default=None)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        raise SystemExit("A component command is required after the launcher options.")

    try:
        if args.backend == "windows":
            exit_code = run_windows(command)
        else:
            exit_code = run_tmux(args.tmux_session, args.title, command)
    except (OSError, subprocess.CalledProcessError) as error:
        raise SystemExit(
            f"Could not open terminal for {args.title}: {error}"
        ) from error
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
