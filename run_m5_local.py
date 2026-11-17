#!/usr/bin/env python3
"""Run and verify the complete Milestone 5 scenario in one terminal.

This standard-library-only harness starts RM, GFD, three LFDs, three replicas,
and three continuously sending clients. It injects the three faults from the
rubric, waits for RM-driven recovery and checkpoint catch-up after each one,
and verifies that every client received replies during every recovery window.

It is intentionally a demo/test driver, not a system component. The actual
recovery path remains RM -> GFD -> co-located LFD -> server process.
"""

import argparse
from pathlib import Path
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"


class DemoFailure(RuntimeError):
    """Raised when the local end-to-end scenario misses an expected event."""


class ProcessGroup:
    def __init__(self, show_logs: bool):
        self.show_logs = show_logs
        self.processes = {}
        self.lines = []
        self.condition = threading.Condition()

    def start(self, name: str, argv: list) -> subprocess.Popen:
        process = subprocess.Popen(
            argv,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self.processes[name] = process
        threading.Thread(
            target=self._read_output,
            args=(name, process),
            daemon=True,
        ).start()
        return process

    def _read_output(self, name: str, process: subprocess.Popen) -> None:
        for line in process.stdout:
            line = line.rstrip()
            with self.condition:
                self.lines.append((name, line))
                self.condition.notify_all()
            if self.show_logs:
                print(f"[{name}] {line}", flush=True)

    def cursor(self) -> int:
        with self.condition:
            return len(self.lines)

    def wait_for(
        self,
        text: str,
        start: int = 0,
        timeout: float = 20.0,
        source: str = None,
    ) -> int:
        deadline = time.monotonic() + timeout
        with self.condition:
            while True:
                for index in range(start, len(self.lines)):
                    if (
                        text in self.lines[index][1]
                        and (source is None or self.lines[index][0] == source)
                    ):
                        return index + 1

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    recent = "\n".join(
                        f"[{name}] {line}" for name, line in self.lines[-30:]
                    )
                    raise DemoFailure(
                        f"Timed out waiting for {text!r} from "
                        f"{source or 'any process'}. Recent output:\n{recent}"
                    )
                self.condition.wait(min(remaining, 0.5))

    def assert_client_progress(self, start: int, end: int, label: str) -> None:
        with self.condition:
            window = list(self.lines[start:end])
        for client_id in ("C1", "C2", "C3"):
            if not any(
                name == client_id and "Received <" in line and ", reply>" in line
                for name, line in window
            ):
                raise DemoFailure(
                    f"{client_id} received no reply during {label}; "
                    "continuous operation was not demonstrated."
                )

    def stop_all(self, extra_pids: list) -> None:
        # Recovered replicas are children of LFDs, not direct children of this
        # harness, so stop their recorded PIDs before stopping the LFDs.
        for pid in extra_pids:
            try:
                os.kill(pid, 15)
            except (OSError, ProcessLookupError):
                pass

        for process in reversed(list(self.processes.values())):
            if process.poll() is None:
                process.terminate()

        deadline = time.monotonic() + 3.0
        for process in reversed(list(self.processes.values())):
            if process.poll() is None:
                try:
                    process.wait(max(0.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    process.kill()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_pid(path: Path, old_pid: int = None, timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            pid = int(path.read_text(encoding="utf-8").strip())
            if pid != old_pid:
                return pid
        except (OSError, ValueError):
            pass
        time.sleep(0.1)
    raise DemoFailure(f"No new recovered-replica PID appeared in {path}.")


def server_command(
    replica_id: str,
    port: int,
    mode: str,
    rm_port: int,
) -> list:
    command = [
        sys.executable,
        "-u",
        str(SRC / "server_replica.py"),
        "--replica-id",
        replica_id,
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--replication-mode",
        mode,
        "--rm-host",
        "127.0.0.1",
        "--rm-port",
        str(rm_port),
        "--state-transfer-delay",
        "0.6",
    ]
    if mode == "passive":
        command.extend(["--checkpoint-freq", "0.35"])
    return command


def run_scenario(mode: str, show_logs: bool, first_fault: str) -> None:
    group = ProcessGroup(show_logs)
    rm_port = free_port()
    gfd_port = free_port()
    replica_ports = {f"S{i}": free_port() for i in range(1, 4)}
    launch_order = [first_fault] + [
        replica_id for replica_id in replica_ports if replica_id != first_fault
    ]
    recovered_pids = []

    with tempfile.TemporaryDirectory(prefix="cmu18749-m5-") as temp_dir:
        pid_files = {
            replica_id: Path(temp_dir) / f"{replica_id}.pid"
            for replica_id in replica_ports
        }

        try:
            group.start(
                "RM",
                [
                    sys.executable,
                    "-u",
                    str(SRC / "replication_manager.py"),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(rm_port),
                    "--replication-mode",
                    mode,
                    "--auto-recovery",
                    "--recovery-retry",
                    "0.4",
                ],
            )
            group.wait_for("RM: 0 members", timeout=8.0)

            group.start(
                "GFD",
                [
                    sys.executable,
                    "-u",
                    str(SRC / "global_fault_detector.py"),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(gfd_port),
                    "--rm-host",
                    "127.0.0.1",
                    "--rm-port",
                    str(rm_port),
                    "--lfd-timeout",
                    "2",
                ],
            )
            group.wait_for("GFD: 0 members", timeout=8.0)

            for index, (replica_id, replica_port) in enumerate(
                replica_ports.items(), start=1
            ):
                recovery_command = server_command(
                    replica_id,
                    replica_port,
                    mode,
                    rm_port,
                )
                group.start(
                    f"LFD{index}",
                    [
                        sys.executable,
                        "-u",
                        str(SRC / "local_fault_detector.py"),
                        "--lfd-id",
                        f"LFD{index}",
                        "--replica-id",
                        replica_id,
                        "--server-host",
                        "127.0.0.1",
                        "--server-port",
                        str(replica_port),
                        "--heartbeat-freq",
                        "0.25",
                        "--gfd-host",
                        "127.0.0.1",
                        "--gfd-port",
                        str(gfd_port),
                        "--replica-public-host",
                        "127.0.0.1",
                        "--recovery-pid-file",
                        str(pid_files[replica_id]),
                        "--recovery-command",
                        *recovery_command,
                    ],
                )

            initial_servers = {}
            for launch_index, replica_id in enumerate(launch_order):
                replica_port = replica_ports[replica_id]
                launch_cursor = group.cursor()
                initial_servers[replica_id] = group.start(
                    replica_id,
                    server_command(replica_id, replica_port, mode, rm_port),
                )
                if launch_index == 0:
                    # Do not let LFD scheduling choose the passive primary by
                    # accident: prove the selected first replica reached RM
                    # membership before any competitor is launched.
                    group.wait_for(
                        f"RM: 1 member: {replica_id}",
                        start=launch_cursor,
                        timeout=10.0,
                    )
                else:
                    time.sleep(0.3)

            group.wait_for(
                f"ready = {', '.join(launch_order)}",
                timeout=25.0,
            )

            for index in range(1, 4):
                group.start(
                    f"C{index}",
                    [
                        sys.executable,
                        "-u",
                        str(SRC / "client.py"),
                        "--client-id",
                        f"C{index}",
                        "--gfd-host",
                        "127.0.0.1",
                        "--gfd-port",
                        str(gfd_port),
                        "--replication-mode",
                        mode,
                        "--add-amount",
                        str(index),
                        "--request-interval",
                        "0.15",
                        "--reply-timeout-ms",
                        "800",
                    ],
                )
            for client_id in ("C1", "C2", "C3"):
                group.wait_for("Received <", timeout=10.0, source=client_id)

            # Fault 1: in passive mode the requested target was launched first
            # and is therefore the (non-hard-coded) primary.
            cursor = group.cursor()
            initial_servers[first_fault].kill()
            ready_cursor = group.wait_for(
                "i_am_ready = 1 after checkpoint",
                start=cursor,
                timeout=25.0,
            )
            group.assert_client_progress(cursor, ready_cursor, "first recovery")
            first_recovered_pid = wait_for_pid(pid_files[first_fault])
            recovered_pids.append(first_recovered_pid)

            if mode == "passive":
                group.wait_for(
                    f"elected new primary {launch_order[1]}",
                    start=cursor,
                    timeout=10.0,
                )

            # Fault 2: kill the same process RM just recovered.
            cursor = group.cursor()
            os.kill(first_recovered_pid, 15)
            recovered_pids.remove(first_recovered_pid)
            ready_cursor = group.wait_for(
                "i_am_ready = 1 after checkpoint",
                start=cursor,
                timeout=25.0,
            )
            group.assert_client_progress(cursor, ready_cursor, "second recovery")
            second_recovered_pid = wait_for_pid(
                pid_files[first_fault],
                old_pid=first_recovered_pid,
            )
            recovered_pids.append(second_recovered_pid)

            # Fault 3: kill a different active replica, or a different backup
            # in passive mode (S2 is now primary, so choose S3).
            third_replica = launch_order[1] if mode == "active" else launch_order[2]
            cursor = group.cursor()
            initial_servers[third_replica].kill()
            ready_cursor = group.wait_for(
                "i_am_ready = 1 after checkpoint",
                start=cursor,
                timeout=25.0,
            )
            group.assert_client_progress(cursor, ready_cursor, "third recovery")
            recovered_pids.append(wait_for_pid(pid_files[third_replica]))

            group.wait_for("RM: 3 members:", start=cursor, timeout=10.0)
            print(
                f"PASS: {mode} replication survived all three faults, "
                "auto-recovered to three ready replicas each time, and all "
                "clients continued receiving replies.",
                flush=True,
            )
        finally:
            # Also scan every PID file so a failure between launch and an
            # assertion cannot leave an automatically recovered child behind.
            for pid_file in pid_files.values():
                try:
                    pid = int(pid_file.read_text(encoding="utf-8").strip())
                    if pid not in recovered_pids:
                        recovered_pids.append(pid)
                except (OSError, ValueError):
                    pass
            group.stop_all(recovered_pids)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the one-terminal Milestone 5 integration scenario."
    )
    parser.add_argument(
        "--mode",
        choices=["active", "passive"],
        required=True,
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Show only the final PASS or failure instead of multiplexed logs.",
    )
    parser.add_argument(
        "--first-fault",
        choices=["S1", "S2", "S3"],
        default="S1",
        help="Replica to kill first (default: S1). In passive mode it is "
             "launched first and is therefore the initial primary.",
    )
    args = parser.parse_args()

    try:
        run_scenario(
            args.mode,
            show_logs=not args.quiet,
            first_fault=args.first_fault,
        )
    except (DemoFailure, OSError) as error:
        raise SystemExit(f"FAIL: {error}")


if __name__ == "__main__":
    main()
