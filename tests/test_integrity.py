from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from dummer.integrity import (
    IntegrityOptions,
    compare_file_bytes,
    eligible_integrity_dirs,
    run_integrity_check,
)


class _BytesResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class IntegrityTests(unittest.TestCase):
    def test_compare_file_bytes_detects_mismatch_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "one.dat"
            path.write_bytes(b"abc123")

            def opener(_bucket: str, _key: str, _region: str | None):
                return _BytesResponse(b"abc124")

            result = compare_file_bytes(
                local_path=path,
                bucket="bucket",
                key="root/one.dat",
                region=None,
                chunk_size=2,
                opener=opener,
            )

            self.assertEqual(result.status, "mismatch")
            self.assertGreater(result.bytes_compared, 0)

    def test_eligible_dirs_are_complete_processed_dirs_only(self) -> None:
        self.assertEqual(
            eligible_integrity_dirs(
                {"done": 2, "pending": 3, "empty": 0},
                {"done": 2, "pending": 1, "extra": 7},
            ),
            ["done"],
        )

    def test_run_integrity_check_writes_unique_report_and_streams_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            local_root = base / "bundle"
            target = local_root / "collection" / "day1"
            report_dir = base / "reports"
            target.mkdir(parents=True)
            (target / "a.dat").write_bytes(b"alpha")
            (target / "b.dat").write_bytes(b"beta")

            listing_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents><Key>archive/bundle/collection/day1/a.dat</Key></Contents>
  <Contents><Key>archive/bundle/collection/day1/b.dat</Key></Contents>
</ListBucketResult>
"""

            def list_urlopen(_request, timeout=30):
                return _BytesResponse(listing_xml)

            def opener(_bucket: str, key: str, _region: str | None):
                payload = b"alpha" if key.endswith("a.dat") else b"beta"
                return _BytesResponse(payload)

            output = io.StringIO()
            with redirect_stdout(output):
                with patch("dummer.integrity.urlopen", side_effect=list_urlopen):
                    result = run_integrity_check(
                        local_path=str(local_root),
                        local_inventory={"collection/day1": 2},
                        processed_inventory={"collection/day1": 2},
                        processed_s3_bucket="example-bucket",
                        processed_s3_prefix="archive/bundle/",
                        processed_root="archive/bundle",
                        processed_s3_region=None,
                        include_hidden=False,
                        options=IntegrityOptions(enabled=True, report_dir=report_dir),
                        opener=opener,
                    )

            self.assertEqual(result.status, "succeeded")
            self.assertEqual(result.files_checked, 2)
            self.assertIsNotNone(result.report_path)
            assert result.report_path is not None
            payload = json.loads(result.report_path.read_text(encoding="utf-8"))
            self.assertFalse(payload["downloaded_to_disk"])
            self.assertEqual(payload["directories"][0]["status"], "matched")
            log_text = output.getvalue()
            self.assertIn("Integrity check stage starting.", log_text)
            self.assertIn("Integrity check probability decision:", log_text)
            self.assertIn("Integrity check starting directory", log_text)
            self.assertIn("Integrity check finished directory", log_text)

    def test_probability_gate_skips_and_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports"
            result = run_integrity_check(
                local_path=tmp,
                local_inventory={"day": 1},
                processed_inventory={"day": 1},
                processed_s3_bucket="example-bucket",
                processed_s3_prefix="archive/",
                processed_root="archive",
                processed_s3_region=None,
                include_hidden=False,
                options=IntegrityOptions(enabled=True, report_dir=report_dir, run_probability=0.0),
            )

            self.assertEqual(result.status, "skipped")
            self.assertIsNotNone(result.report_path)
            reports = list(report_dir.glob("integrity_report_*.json"))
            self.assertEqual(len(reports), 1)

    def test_run_integrity_check_fails_and_reports_byte_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            local_root = base / "bundle"
            target = local_root / "collection" / "day1"
            report_dir = base / "reports"
            target.mkdir(parents=True)
            (target / "a.dat").write_bytes(b"alpha")
            (target / "b.dat").write_bytes(b"beta")

            listing_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Contents><Key>archive/bundle/collection/day1/a.dat</Key></Contents>
  <Contents><Key>archive/bundle/collection/day1/b.dat</Key></Contents>
</ListBucketResult>
"""

            def list_urlopen(_request, timeout=30):
                return _BytesResponse(listing_xml)

            def opener(_bucket: str, key: str, _region: str | None):
                payload = b"alpha" if key.endswith("a.dat") else b"BETA"
                return _BytesResponse(payload)

            output = io.StringIO()
            with redirect_stdout(output):
                with patch("dummer.integrity.urlopen", side_effect=list_urlopen):
                    result = run_integrity_check(
                        local_path=str(local_root),
                        local_inventory={"collection/day1": 2},
                        processed_inventory={"collection/day1": 2},
                        processed_s3_bucket="example-bucket",
                        processed_s3_prefix="archive/bundle/",
                        processed_root="archive/bundle",
                        processed_s3_region=None,
                        include_hidden=False,
                        options=IntegrityOptions(enabled=True, report_dir=report_dir),
                        opener=opener,
                    )

            self.assertEqual(result.status, "failed")
            self.assertEqual(result.failures, 1)
            self.assertIsNotNone(result.report_path)
            assert result.report_path is not None
            payload = json.loads(result.report_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "failed")
            self.assertEqual(payload["directories"][0]["status"], "failed")
            self.assertEqual(payload["directories"][0]["mismatches"][0]["rel_path"], "collection/day1/b.dat")
            self.assertEqual(payload["directories"][0]["mismatches"][0]["status"], "mismatch")
            self.assertIn("Integrity mismatch detected", output.getvalue())
