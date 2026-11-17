"""Focused unit tests for the Milestone 5 recovery control path."""

from pathlib import Path
import sys
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import global_fault_detector
import local_fault_detector
import protocol
import replication_manager


class ProtocolTests(unittest.TestCase):
    def test_recovery_messages_round_trip(self):
        hello = protocol.parse_message(
            protocol.build_recovery_hello("LFD2", "S2")
        )
        self.assertEqual(
            hello,
            {
                "type": protocol.RECOVERY_HELLO,
                "lfd_id": "LFD2",
                "replica_id": "S2",
            },
        )

        recover = protocol.parse_message(
            protocol.build_recover_replica("S2", 17)
        )
        self.assertEqual(recover["type"], protocol.RECOVER_REPLICA)
        self.assertEqual(recover["replica_id"], "S2")
        self.assertEqual(recover["recovery_epoch"], 17)


class ReplicationManagerTests(unittest.TestCase):
    @staticmethod
    def membership(*replica_ids):
        return {
            "type": protocol.MEMBERSHIP,
            "members": [
                (replica_id, "127.0.0.1", 5000 + index)
                for index, replica_id in enumerate(replica_ids, start=1)
            ],
            "primary_id": None,
        }

    def test_failure_starts_recovery_and_rejoin_completes_it(self):
        manager = replication_manager.ReplicationManager(
            "passive",
            auto_recovery=True,
            recovery_retry_sec=10,
        )
        gfd_conn = object()
        sent = []

        with mock.patch.object(
            protocol,
            "send_line",
            side_effect=lambda _conn, line: sent.append(line),
        ):
            manager._handle_gfd_membership(
                gfd_conn,
                self.membership("S1", "S2", "S3"),
            )
            self.assertEqual(manager.primary_id, "S1")

            sent.clear()
            manager._handle_gfd_membership(
                gfd_conn,
                self.membership("S2", "S3"),
            )

            self.assertIn("S1", manager._pending_recoveries)
            self.assertEqual(manager.primary_id, "S2")
            recovery_lines = [
                line
                for line in sent
                if protocol.parse_message(line)["type"]
                == protocol.RECOVER_REPLICA
            ]
            self.assertEqual(len(recovery_lines), 1)
            self.assertEqual(
                protocol.parse_message(recovery_lines[0])["replica_id"],
                "S1",
            )

            manager._handle_gfd_membership(
                gfd_conn,
                self.membership("S2", "S3", "S1"),
            )
            self.assertNotIn("S1", manager._pending_recoveries)
            self.assertEqual(manager.primary_id, "S2")

    def test_manual_m4_mode_does_not_request_recovery(self):
        manager = replication_manager.ReplicationManager("active")
        gfd_conn = object()
        sent = []

        with mock.patch.object(
            protocol,
            "send_line",
            side_effect=lambda _conn, line: sent.append(line),
        ):
            manager._handle_gfd_membership(
                gfd_conn,
                self.membership("S1"),
            )
            manager._handle_gfd_membership(gfd_conn, self.membership())

        message_types = [protocol.parse_message(line)["type"] for line in sent]
        self.assertNotIn(protocol.RECOVER_REPLICA, message_types)


class RelayTests(unittest.TestCase):
    def test_gfd_relays_recovery_to_registered_lfd(self):
        gfd = global_fault_detector.GlobalFaultDetector()
        lfd_conn = object()
        gfd._recovery_agents["S3"] = lfd_conn
        sent = []

        with mock.patch.object(
            protocol,
            "send_line",
            side_effect=lambda conn, line: sent.append((conn, line)),
        ):
            gfd._forward_recovery_request(
                {
                    "type": protocol.RECOVER_REPLICA,
                    "replica_id": "S3",
                    "recovery_epoch": 9,
                }
            )

        self.assertEqual(sent[0][0], lfd_conn)
        parsed = protocol.parse_message(sent[0][1])
        self.assertEqual(parsed["replica_id"], "S3")
        self.assertEqual(parsed["recovery_epoch"], 9)


class RecoveryAgentTests(unittest.TestCase):
    def test_duplicate_request_does_not_launch_duplicate_process(self):
        process = mock.Mock(pid=1234)
        process.poll.return_value = None
        agent = local_fault_detector.ReplicaRecoveryAgent(
            "LFD1",
            "S1",
            ["python", "server_replica.py"],
        )

        with mock.patch.object(
            local_fault_detector.subprocess,
            "Popen",
            return_value=process,
        ) as popen:
            agent.recover(4)
            agent.recover(4)

        popen.assert_called_once_with(
            ["python", "server_replica.py"],
            shell=False,
        )

    def test_failed_launch_is_nonfatal_and_can_be_retried(self):
        process = mock.Mock(pid=5678)
        process.poll.return_value = None
        agent = local_fault_detector.ReplicaRecoveryAgent(
            "LFD1",
            "S1",
            ["missing-program"],
        )

        with mock.patch.object(
            local_fault_detector.subprocess,
            "Popen",
            side_effect=[OSError("not found"), process],
        ) as popen:
            agent.recover(2)
            agent.recover(2)

        self.assertEqual(popen.call_count, 2)


if __name__ == "__main__":
    unittest.main()
