from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from dummer.inventory import (
    _path_contains_component,
    _resume_trusted_counts_and_cluster,
    crawl_inventory_to_state_file_with_options,
    parse_inventory_manifest_to_state_file,
    _load_known_s3_directories,
)
from dummer.state import read_state_file


class InventoryTests(unittest.TestCase):
    def test_crawl_counts_root_files_as_dot_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            child = base / "collection"
            child.mkdir(parents=True)
            (base / "root_a.dat").write_text("a", encoding="utf-8")
            (base / "root_b.dat").write_text("b", encoding="utf-8")
            (child / "child.dat").write_text("c", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(str(base), out_path)

            self.assertEqual(written, 2)
            self.assertEqual(read_state_file(out_path), {".": 2, "collection": 1})

    def test_crawl_ignores_hidden_files_and_directories_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            child = base / "collection"
            hidden_child = base / ".hidden_collection"
            child.mkdir(parents=True)
            hidden_child.mkdir(parents=True)
            (base / "root.dat").write_text("a", encoding="utf-8")
            (base / ".root_hidden.dat").write_text("hidden", encoding="utf-8")
            (child / "child.dat").write_text("b", encoding="utf-8")
            (child / ".child_hidden.dat").write_text("hidden", encoding="utf-8")
            (hidden_child / "hidden_child.dat").write_text("hidden", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(str(base), out_path)

            self.assertEqual(written, 2)
            self.assertEqual(read_state_file(out_path), {".": 1, "collection": 1})

    def test_crawl_can_include_hidden_files_and_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            child = base / "collection"
            hidden_child = base / ".hidden_collection"
            child.mkdir(parents=True)
            hidden_child.mkdir(parents=True)
            (base / "root.dat").write_text("a", encoding="utf-8")
            (base / ".root_hidden.dat").write_text("hidden", encoding="utf-8")
            (child / ".child_hidden.dat").write_text("hidden", encoding="utf-8")
            (hidden_child / "hidden_child.dat").write_text("hidden", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(str(base), out_path, include_hidden=True)

            self.assertEqual(written, 3)
            self.assertEqual(read_state_file(out_path), {".": 2, ".hidden_collection": 1, "collection": 1})

    def test_crawl_max_depth_zero_keeps_only_root_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            child = base / "collection"
            child.mkdir(parents=True)
            (base / "root.dat").write_text("a", encoding="utf-8")
            (child / "child.dat").write_text("b", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(
                str(base),
                out_path,
                crawl_max_depth=0,
            )

            self.assertEqual(written, 1)
            self.assertEqual(read_state_file(out_path), {".": 1})

    def test_path_filter_excludes_dot_root_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            child = base / "collection" / "2026"
            child.mkdir(parents=True)
            (base / "root.dat").write_text("a", encoding="utf-8")
            (child / "child.dat").write_text("b", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(str(base), out_path, path_filter="2026")

            self.assertEqual(written, 1)
            self.assertEqual(read_state_file(out_path), {"collection/2026": 1})

    def test_manifest_counts_root_files_as_dot_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.txt"
            manifest.write_text(
                "\n".join(
                    [
                        "bundle/root_a.dat",
                        "bundle/root_b.dat",
                        "bundle/collection/child.dat",
                    ]
                ),
                encoding="utf-8",
            )

            out_path = Path(tmp) / "state.txt"
            written = parse_inventory_manifest_to_state_file(str(manifest), out_path)

            self.assertEqual(written, 2)
            self.assertEqual(read_state_file(out_path), {".": 2, "collection": 1})

    def test_manifest_ignores_hidden_path_components_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.txt"
            manifest.write_text(
                "\n".join(
                    [
                        "bundle/root.dat",
                        "bundle/.root_hidden.dat",
                        "bundle/collection/.hidden.dat",
                        "bundle/.hidden_collection/file.dat",
                    ]
                ),
                encoding="utf-8",
            )

            out_path = Path(tmp) / "state.txt"
            written = parse_inventory_manifest_to_state_file(str(manifest), out_path)

            self.assertEqual(written, 1)
            self.assertEqual(read_state_file(out_path), {".": 1})

    def test_manifest_can_include_hidden_path_components(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.txt"
            manifest.write_text(
                "\n".join(
                    [
                        "bundle/root.dat",
                        "bundle/.root_hidden.dat",
                        "bundle/collection/.hidden.dat",
                        "bundle/.hidden_collection/file.dat",
                    ]
                ),
                encoding="utf-8",
            )

            out_path = Path(tmp) / "state.txt"
            written = parse_inventory_manifest_to_state_file(str(manifest), out_path, include_hidden=True)

            self.assertEqual(written, 3)
            self.assertEqual(read_state_file(out_path), {".": 2, ".hidden_collection": 1, "collection": 1})

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

    def test_crawl_max_depth_limits_recursive_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            child = base / "collection"
            grandchild = child / "2026"
            great_grandchild = grandchild / "leaf"
            great_grandchild.mkdir(parents=True)
            (child / "a.dat").write_text("a", encoding="utf-8")
            (grandchild / "b.dat").write_text("b", encoding="utf-8")
            (great_grandchild / "c.dat").write_text("c", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(
                str(base),
                out_path,
                crawl_max_depth=1,
            )

            self.assertEqual(written, 1)
            self.assertEqual(read_state_file(out_path), {"collection": 1})

    def test_crawl_min_and_max_depth_keep_only_depth_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            child = base / "collection"
            grandchild = child / "2026"
            great_grandchild = grandchild / "leaf"
            great_grandchild.mkdir(parents=True)
            (child / "a.dat").write_text("a", encoding="utf-8")
            (grandchild / "b.dat").write_text("b", encoding="utf-8")
            (great_grandchild / "c.dat").write_text("c", encoding="utf-8")

            out_path = Path(tmp) / "state.txt"
            written = crawl_inventory_to_state_file_with_options(
                str(base),
                out_path,
                crawl_min_depth=2,
                crawl_max_depth=2,
            )

            self.assertEqual(written, 1)
            self.assertEqual(read_state_file(out_path), {"collection/2026": 1})

    def test_crawl_min_depth_cannot_exceed_max_depth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            base.mkdir()
            out_path = Path(tmp) / "state.txt"

            with self.assertRaisesRegex(ValueError, "cannot be greater"):
                crawl_inventory_to_state_file_with_options(
                    str(base),
                    out_path,
                    crawl_min_depth=3,
                    crawl_max_depth=2,
                )

    def test_crawl_depth_cannot_be_negative(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle"
            base.mkdir()
            out_path = Path(tmp) / "state.txt"

            with self.assertRaisesRegex(ValueError, "0 or greater"):
                crawl_inventory_to_state_file_with_options(
                    str(base),
                    out_path,
                    crawl_min_depth=-1,
                )

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
