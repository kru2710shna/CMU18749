"""Tests for isolated-terminal recovery demo tooling without opening a UI."""

from pathlib import Path
import socket
import subprocess
import sys
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import launch_component_terminal


class TerminalLauncherTests(unittest.TestCase):
    def test_lfd_accepts_recovery_command_without_separator(self):
        """Keep nested server options inside argparse.REMAINDER on every OS."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            unavailable_gfd_port = probe.getsockname()[1]

        command = [
            sys.executable,
            "-u",
            str(ROOT / "src" / "local_fault_detector.py"),
            "--lfd-id",
            "LFD1",
            "--replica-id",
            "S1",
            "--server-host",
            "127.0.0.1",
            "--server-port",
            "59991",
            "--heartbeat-freq",
            "0.1",
            "--gfd-host",
            "127.0.0.1",
            "--gfd-port",
            str(unavailable_gfd_port),
            "--replica-public-host",
            "127.0.0.1",
            "--recovery-command",
            sys.executable,
            "-u",
            str(ROOT / "launch_component_terminal.py"),
            "--backend",
            "windows",
            "--title",
            "S1-recovered",
            sys.executable,
            "-u",
            str(ROOT / "src" / "server_replica.py"),
            "--replica-id",
            "S1",
            "--host",
            "127.0.0.1",
            "--port",
            "59991",
            "--replication-mode",
            "active",
            "--rm-host",
            "127.0.0.1",
            "--rm-port",
            "59992",
            "--state-transfer-delay",
            "0",
        ]

        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(0.4)
            self.assertIsNone(
                process.poll(),
                "LFD rejected the nested recovery command during argument parsing",
            )
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

    def test_windows_backend_creates_new_console_and_waits(self):
        child = mock.Mock()
        child.wait.return_value = 0

        with mock.patch.object(launch_component_terminal.os, "name", "nt"), \
             mock.patch.object(
                 launch_component_terminal.subprocess,
                 "Popen",
                 return_value=child,
             ) as popen:
            result = launch_component_terminal.run_windows(
                ["python", "server_replica.py"]
            )

        self.assertEqual(result, 0)
        popen.assert_called_once_with(
            ["python", "server_replica.py"],
            creationflags=launch_component_terminal.subprocess.CREATE_NEW_CONSOLE,
        )
        child.wait.assert_called_once_with()

    def test_tmux_backend_opens_window_and_waits_for_pane_to_close(self):
        opened = mock.Mock(returncode=0, stdout="%9\n")
        pane_exists = mock.Mock(returncode=0, stdout="%9\n")
        pane_closed = mock.Mock(returncode=1, stdout="")

        with mock.patch.object(
            launch_component_terminal.subprocess,
            "run",
            side_effect=[opened, pane_exists, pane_closed],
        ) as run, mock.patch.object(launch_component_terminal.time, "sleep"):
            result = launch_component_terminal.run_tmux(
                "m5-local",
                "S1-recovered",
                ["python3", "server replica.py", "--replica-id", "S1"],
            )

        self.assertEqual(result, 0)
        open_command = run.call_args_list[0].args[0]
        self.assertEqual(open_command[0:2], ["tmux", "new-window"])
        self.assertIn("m5-local", open_command)
        self.assertIn("S1-recovered", open_command)
        self.assertIn("'server replica.py'", open_command[-1])
        self.assertEqual(run.call_args_list[-1].args[0][-2:], ["%9", "#{pane_id}"])


if __name__ == "__main__":
    unittest.main()
