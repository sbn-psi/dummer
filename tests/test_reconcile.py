from __future__ import annotations

import unittest

from dummer.reconcile import reconcile, summarize_reconciliation


class ReconcileTests(unittest.TestCase):
    def test_reconcile_marks_missing_and_mismatched_directories_pending(self) -> None:
        result = reconcile(
            {
                "calibration/703/2026/26Apr04": 10,
                "calibration/703/2026/26Apr05": 12,
                "calibration/703/2026/26Apr06": 12,
            },
            {
                "calibration/703/2026/26Apr04": 10,
                "calibration/703/2026/26Apr05": 8,
                "calibration/703/2026/26Apr06": 13,
            },
        )

        self.assertEqual([item.rel_dir for item in result.pending], ["calibration/703/2026/26Apr05"])
        self.assertEqual([item.rel_dir for item in result.drift], ["calibration/703/2026/26Apr06"])

    def test_summarize_reconciliation_groups_all_paths_by_default(self) -> None:
        summary = summarize_reconciliation(
            {
                "calibration": 2688,
                "calibration/703/2026/26Apr04": 10,
                "calibration/703/2026/26Apr05": 12,
                "calibration/G96/2026/26Apr04": 7,
                "miscellaneous/V06/2025/25Dec23": 4,
            },
            {
                "calibration": 2688,
                "calibration/703/2026/26Apr04": 10,
                "calibration/703/2026/26Apr05": 8,
                "calibration/G96/2026/26Apr04": 7,
            },
        )

        self.assertEqual(
            summary,
            [
                {
                    "anchor": "all",
                    "directories": {"processed": 3, "total": 5, "missing": 2},
                    "files": {"processed": 2713, "total": 2721, "missing": 8},
                },
            ],
        )

    def test_summarize_reconciliation_can_anchor_by_configured_component(self) -> None:
        summary = summarize_reconciliation(
            {
                "calibration/703/2026/26Apr04": 10,
                "miscellaneous/V06/2025/25Dec23": 4,
            },
            {},
            summary_anchor_component=2,
        )

        self.assertEqual([bucket["anchor"] for bucket in summary], ["2025", "2026"])

    def test_summarize_reconciliation_can_group_by_configured_components(self) -> None:
        summary = summarize_reconciliation(
            {
                "calibration/703/2026/26Apr04": 10,
                "calibration/G96/2026/26Apr04": 7,
            },
            {
                "calibration/703/2026/26Apr04": 10,
            },
            group_component_indices=(0, 1),
        )

        self.assertEqual(
            summary,
            [
                {
                    "anchor": "all",
                    "directories": {"processed": 1, "total": 1, "missing": 0},
                    "files": {"processed": 10, "total": 10, "missing": 0},
                    "group": {"component_0": "calibration", "component_1": "703"},
                },
                {
                    "anchor": "all",
                    "directories": {"processed": 0, "total": 1, "missing": 1},
                    "files": {"processed": 0, "total": 7, "missing": 7},
                    "group": {"component_0": "calibration", "component_1": "G96"},
                },
            ],
        )

    def test_summarize_reconciliation_can_use_labeled_group_components(self) -> None:
        summary = summarize_reconciliation(
            {
                "calibration/703/2026/26Apr04": 10,
            },
            {},
            group_components=(("collection", 0), ("instrument", 1)),
        )

        self.assertEqual(summary[0]["group"], {"collection": "calibration", "instrument": "703"})


if __name__ == "__main__":
    unittest.main()
