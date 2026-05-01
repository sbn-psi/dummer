from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from dummer.inventory import (
    _path_contains_component,
    _resume_trusted_counts_and_cluster,
    crawl_inventory_to_state_file_with_options,
    _load_known_s3_directories,
)
from dummer.state import read_state_file


class InventoryTests(unittest.TestCase):
    def test_crawl_counts_direct_files_for_non_leaf_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            parent = base / "collection" / "2025" / "parent"
            child = parent / "child"
            child.mkdir(parents=True)
            (parent / "a.dat").write_text("a", encoding="utf-8")
            (parent / "b.dat").write_text("b", encoding="utf-8")
            (child / "c.dat").write_text("c", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(str(base), out_path, path_filter="2025")

            self.assertEqual(written, 2)
            self.assertEqual(
                read_state_file(out_path),
                {
                    "collection/2025/parent": 2,
                    "collection/2025/parent/child": 1,
                },
            )

    def test_known_s3_directories_keep_parents_and_children(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            known_dirs = Path(tmp) / "known_dirs.txt"
            known_dirs.write_text(
                "\n".join(
                    [
                        "sbn/gbo.ast.catalina.survey/collection/2025/parent",
                        "sbn/gbo.ast.catalina.survey/collection/2025/parent/child",
                    ]
                ),
                encoding="utf-8",
            )

            entries = _load_known_s3_directories(
                str(known_dirs),
                bucket_prefix="sbn/gbo.ast.catalina.survey/",
                bundle_root="sbn/gbo.ast.catalina.survey",
                path_filter="2025",
            )

            self.assertEqual(
                [entry.rel_dir for entry in entries],
                [
                    "collection/2025/parent",
                    "collection/2025/parent/child",
                ],
            )

    def test_path_component_match_handles_leading_slash(self) -> None:
        self.assertTrue(_path_contains_component("/2025/foo/bar.dat", "2025"))
        self.assertTrue(_path_contains_component("/bundle/2025/foo/bar.dat", "2025"))
        self.assertFalse(_path_contains_component("/bundle/12025/foo/bar.dat", "2025"))

    def test_path_filter_does_not_prune_without_depth_hint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            nested = base / "collection" / "not-the-filter" / "2025" / "parent"
            nested.mkdir(parents=True)
            (nested / "a.dat").write_text("a", encoding="utf-8")

            no_prune_out = Path(tmp) / "no_prune.txt"
            pruned_out = Path(tmp) / "pruned.txt"

            no_prune_written = crawl_inventory_to_state_file_with_options(
                str(base),
                no_prune_out,
                path_filter="2025",
            )
            pruned_written = crawl_inventory_to_state_file_with_options(
                str(base),
                pruned_out,
                path_filter="2025",
                path_filter_depth=1,
            )

            self.assertEqual(no_prune_written, 1)
            self.assertEqual(read_state_file(no_prune_out), {"collection/not-the-filter/2025/parent": 1})
            self.assertEqual(pruned_written, 0)
            self.assertEqual(read_state_file(pruned_out), {})

    def test_resume_cluster_depth_is_configurable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state.txt"
            state.write_text(
                "\n".join(
                    [
                        "collection/703/2026/one\t1",
                        "collection/G96/2026/two\t2",
                    ]
                ),
                encoding="utf-8",
            )

            trusted_depth_1, cluster_depth_1 = _resume_trusted_counts_and_cluster(state, cluster_depth=1)
            trusted_depth_2, cluster_depth_2 = _resume_trusted_counts_and_cluster(state, cluster_depth=2)

            self.assertEqual(cluster_depth_1, "collection")
            self.assertEqual(trusted_depth_1, {})
            self.assertEqual(cluster_depth_2, "collection/G96")
            self.assertEqual(trusted_depth_2, {"collection/703/2026/one": 1})


if __name__ == "__main__":
    unittest.main()
