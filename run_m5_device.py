#!/usr/bin/env python3
"""Cross-platform, one-terminal-per-machine Milestone 5 demo launcher.

Sacred machine:
    python run_m5_device.py sacred active

Each replica machine (replace address and number):
    python run_m5_device.py replica 1 active 192.0.2.10

The sacred console accepts ``clients`` and ``quit``. A replica console accepts
``start``, ``kill``, ``status``, and ``quit``. This keeps every component a
separate OS process while avoiding tmux and working on Windows, macOS, and
Linux. Recovery itself is not performed by this launcher: the RM decision is
relayed by the GFD to the LFD, which launches the configured replica argv.
"""

import argparse
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"


class ConsoleProcesses:
    def __init__(self):
        self.processes = {}

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
            target=self._print_output,
            args=(name, process),
            daemon=True,
        ).start()
        return process

    @staticmethod
    def _print_output(name: str, process: subprocess.Popen) -> None:
        for line in process.stdout:
            print(f"[{name}] {line.rstrip()}", flush=True)

    def stop(self) -> None:
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


def server_command(
    replica_id: str,
    port: int,
    mode: str,
    sacred_ip: str,
    rm_port: int,
    checkpoint_freq: float,
    state_transfer_delay: float,
) -> list:
    command = [
        sys.executable,
        "-u",
        str(SRC / "server_replica.py"),
        "--replica-id",
        replica_id,
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--replication-mode",
        mode,
        "--rm-host",
        sacred_ip,
        "--rm-port",
        str(rm_port),
        "--state-transfer-delay",
        str(state_transfer_delay),
    ]
    if mode == "passive":
        command.extend(["--checkpoint-freq", str(checkpoint_freq)])
    return command


def run_sacred(args) -> None:
    processes = ConsoleProcesses()
    clients_started = False
    try:
        processes.start(
            "RM",
            [
                sys.executable,
                "-u",
                str(SRC / "replication_manager.py"),
                "--host",
                "0.0.0.0",
                "--port",
                str(args.rm_port),
                "--replication-mode",
                args.mode,
                "--auto-recovery",
                "--recovery-retry",
                str(args.recovery_retry),
            ],
        )
        time.sleep(0.5)
        processes.start(
            "GFD",
            [
                sys.executable,
                "-u",
                str(SRC / "global_fault_detector.py"),
                "--host",
                "0.0.0.0",
                "--port",
                str(args.gfd_port),
                "--rm-host",
                "127.0.0.1",
                "--rm-port",
                str(args.rm_port),
                "--lfd-timeout",
                str(args.lfd_timeout),
            ],
        )
        print(
            "Sacred infrastructure is running. Start all three replicas in "
            "the desired order, then enter 'clients'.",
            flush=True,
        )
        while True:
            command = input("m5-sacred> ").strip().lower()
            if command == "clients":
                if clients_started:
                    print("Clients are already running.")
                    continue
                for index in range(1, 4):
                    processes.start(
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
                            str(args.gfd_port),
                            "--replication-mode",
                            args.mode,
                            "--add-amount",
                            str(index),
                            "--request-interval",
                            str(args.request_interval),
                        ],
                    )
                clients_started = True
            elif command == "quit":
                return
            elif command:
                print("Commands: clients, quit")
    except (EOFError, KeyboardInterrupt):
        return
    finally:
        processes.stop()


def run_replica(args) -> None:
    processes = ConsoleProcesses()
    replica_id = f"S{args.replica_number}"
    lfd_id = f"LFD{args.replica_number}"
    replica_port = args.replica_base_port + args.replica_number
    initial_process = None

    with tempfile.TemporaryDirectory(prefix=f"cmu18749-{replica_id}-") as temp:
        pid_file = Path(temp) / f"{replica_id}.pid"
        launch_command = server_command(
            replica_id,
            replica_port,
            args.mode,
            args.sacred_ip,
            args.rm_port,
            args.checkpoint_freq,
            args.state_transfer_delay,
        )
        try:
            processes.start(
                lfd_id,
                [
                    sys.executable,
                    "-u",
                    str(SRC / "local_fault_detector.py"),
                    "--lfd-id",
                    lfd_id,
                    "--replica-id",
                    replica_id,
                    "--server-host",
                    "127.0.0.1",
                    "--server-port",
                    str(replica_port),
                    "--heartbeat-freq",
                    str(args.heartbeat_freq),
                    "--gfd-host",
                    args.sacred_ip,
                    "--gfd-port",
                    str(args.gfd_port),
                    "--recovery-pid-file",
                    str(pid_file),
                    "--recovery-command",
                    *launch_command,
                ],
            )
            print(
                f"{lfd_id} is running. Enter 'start' when {replica_id} should "
                "join. Use 'kill' for every fault, including recovered copies.",
                flush=True,
            )
            while True:
                command = input(f"m5-{replica_id}> ").strip().lower()
                if command == "start":
                    if initial_process is not None:
                        print("The initial replica has already been started.")
                    else:
                        initial_process = processes.start(replica_id, launch_command)
                elif command == "kill":
                    if initial_process is not None and initial_process.poll() is None:
                        initial_process.kill()
                        print(f"Killed initial {replica_id} PID {initial_process.pid}.")
                        continue
                    try:
                        recovered_pid = int(
                            pid_file.read_text(encoding="utf-8").strip()
                        )
                        os.kill(recovered_pid, 15)
                        print(f"Killed recovered {replica_id} PID {recovered_pid}.")
                    except (OSError, ValueError):
                        print("No running recovered replica PID is available yet.")
                elif command == "status":
                    initial_status = (
                        "not started" if initial_process is None
                        else "running" if initial_process.poll() is None
                        else f"exited ({initial_process.returncode})"
                    )
                    recovered = "none"
                    try:
                        recovered = pid_file.read_text(encoding="utf-8").strip()
                    except OSError:
                        pass
                    print(
                        f"initial={initial_status}; latest recovered PID={recovered}"
                    )
                elif command == "quit":
                    return
                elif command:
                    print("Commands: start, kill, status, quit")
        except (EOFError, KeyboardInterrupt):
            return
        finally:
            try:
                os.kill(int(pid_file.read_text(encoding="utf-8").strip()), 15)
            except (OSError, ValueError):
                pass
            processes.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run one machine's Milestone 5 processes in one console."
    )
    subparsers = parser.add_subparsers(dest="role", required=True)

    sacred = subparsers.add_parser("sacred")
    sacred.add_argument("mode", choices=["active", "passive"])

    replica = subparsers.add_parser("replica")
    replica.add_argument("replica_number", type=int, choices=[1, 2, 3])
    replica.add_argument("mode", choices=["active", "passive"])
    replica.add_argument("sacred_ip")

    for subparser in (sacred, replica):
        subparser.add_argument("--gfd-port", type=int, default=6000)
        subparser.add_argument("--rm-port", type=int, default=6100)
        subparser.add_argument("--lfd-timeout", type=float, default=5.0)
        subparser.add_argument("--heartbeat-freq", type=float, default=1.0)
        subparser.add_argument("--checkpoint-freq", type=float, default=2.0)
        subparser.add_argument("--request-interval", type=float, default=1.0)
        subparser.add_argument("--recovery-retry", type=float, default=2.0)
        subparser.add_argument("--state-transfer-delay", type=float, default=3.0)
        subparser.add_argument("--replica-base-port", type=int, default=5000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.role == "sacred":
        run_sacred(args)
    else:
        run_replica(args)


if __name__ == "__main__":
    main()
