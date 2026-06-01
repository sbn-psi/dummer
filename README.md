# Dummer

Dummer is a small wrapper around [DUM](https://github.com/NASA-PDS/data-upload-manager) for running large uploads from a local data/bundle directory.

It is aimed at answering one operational question:

> Given the files here, and whatever has already been processed, what still needs
> to be uploaded?

DUM by itself can do a full reconciliation for a large set of organized files, but it requires a full crawl and checksum of every file, and verification with their backend in order to accomplish this. For a large accumulating data set, or on a system that might experience interruptions to DUM runs, this can drastically extend the amount of time it takes to upload and verify a large set of files. Dummer adds statefulness and more intelligent discovery and reconciliation so that it can reliably ensure that a set of files has been uploaded, and can be used in a delivery pipeline to build in resilience and additional reporting.

Dummer keeps track of its own state, and breaks large runs into independently verified chunks by directory. It can build or reuse local file counts, compare them with processed state, run DUM for pending folders, and record successful work for the next run.

## Start Here

For a new setup, run the wizard:

```bash
dummer setup
```

From a source checkout:

```bash
./dummer_setup.py
```

The wizard asks about your data and upload environment, then writes the answers
to a `.env` file. Re-run it later to review or change the saved setup. If you
need more detail while answering, type `help` at any question.

After setup, run:

```bash
dummer
```

To override one saved setting for a single run, pass the matching flag:

```bash
dummer --max-dirs 5
```

`dummer wizard` is an alias for `dummer setup`. Installed packages also provide
`dummer-setup`.

## What You Need To Know

Dummer works with two inventories:

- **Local inventory**: what files exist in your data/bundle directory.
- **Processed inventory**: what files or folders have already reached the
  destination.

Each inventory is represented as a simple state file:

```text
relative/directory/path<TAB>number_of_direct_files
```

For example:

```text
collection/703/2026/26Apr30	42
```

The normal run flow is:

1. Build or reuse local inventory.
2. Build or reuse processed inventory.
3. Compare the two inventories.
4. Upload pending folders.
5. Update processed state after successful uploads.

An optional byte-for-byte public S3 integrity stage can run after the normal
workflow. It is completely off unless explicitly enabled.

The setup wizard is the easiest way to choose the right inventory sources. You
can use a filesystem crawl, an existing file list, an existing state file, a
processed filesystem mirror, or a public S3 bucket depending on what your
environment already has.

## Configuration

`dummer` and `dummer_upload.py` resolve settings in this order:

1. Command-line flags.
2. Environment variables such as `DUMMER_LOCAL_PATH`.
3. A `.env` file found through `--script-dir`, the current directory, or the
   default script directory.
4. Built-in defaults.

`dummer_inventory.py` and `dummer_reconcile.py` are explicit utility scripts.
They do not use the full `.env` configuration surface; pass their options
directly.

Boolean environment values accept `1/0`, `true/false`, `yes/no`, or `on/off`.

## Main Command Reference

```bash
dummer [options]
```

From a source checkout:

```bash
./dummer.py [options]
```

### Required Upload Settings

These settings are required when there is upload work to do.

| Environment variable | Flag | Meaning |
| --- | --- | --- |
| `DUMMER_LOCAL_PATH` | `--local-path` | Data/bundle directory containing the local files to upload. |
| `DUMMER_CONFIG` | `--config` | DUM configuration file passed with `-c`. |
| `DUMMER_NAME` | `--name` | DUM node/name value passed with `-n`. |
| `DUMMER_DUM_BINARY` | `--dum-binary` | DUM executable. Defaults to `/usr/local/bin/pds-ingress-client`. |
| `DUMMER_REPORT_DIR` | `--report-dir` | Directory for DUM JSON reports. |
| `DUMMER_PIPELINE_REPORT_DIR` | `--pipeline-report-dir` | Directory for Dummer run reports. |

### Local Inventory

If neither `--local-state` nor `--local-manifest` is set, Dummer crawls
`--local-path`.

| Environment variable | Flag | Meaning |
| --- | --- | --- |
| `DUMMER_LOCAL_PATH` | `--local-path` | Data/bundle directory. Also used for upload paths. |
| `DUMMER_LOCAL_STATE` | `--local-state` | Existing local state file to reuse. |
| `DUMMER_LOCAL_MANIFEST` | `--local-manifest`, `--inventory-manifest` | Plain-text or `.gz` file list to parse as local inventory. |
| `DUMMER_LOCAL_ROOT` | `--local-root`, `--inventory-manifest-root` | Prefix to strip from local manifest paths before comparison. |

### Processed Inventory

Choose exactly one processed source.

| Environment variable | Flag | Meaning |
| --- | --- | --- |
| `DUMMER_PROCESSED_STATE` | `--processed-state` | Existing processed state file. Successful uploads are recorded here. |
| `DUMMER_PROCESSED_MANIFEST` | `--processed-manifest` | Plain-text or `.gz` file list to parse as processed inventory. |
| `DUMMER_PROCESSED_CRAWL` | `--processed-crawl` | Filesystem directory to crawl as processed inventory. |
| `DUMMER_PROCESSED_ROOT` | `--processed-root` | Prefix to strip from processed paths or S3 keys before comparison. |
| `DUMMER_PROCESSED_S3_BUCKET` | `--processed-s3-bucket` | Public S3 bucket to list for processed inventory. Requires anonymous `ListBucket` access. |
| `DUMMER_PROCESSED_S3_PREFIX` | `--processed-s3-prefix` | S3 key prefix to list. |
| `DUMMER_PROCESSED_S3_REGION` | `--processed-s3-region` | Optional S3 region. |
| `DUMMER_PROCESSED_S3_KNOWN_DIRS_FILE` | `--processed-s3-known-dirs-file` | Existing directory list to use for exact-prefix S3 counts. |
| `DUMMER_PROCESSED_S3_KNOWN_DIRS_WORKERS` | `--processed-s3-known-dirs-workers` | Concurrent S3 exact-prefix checks. Defaults to `10`. |
| `DUMMER_PROCESSED_S3_RESUME_FROM_STATE` | `--processed-s3-resume-from-state` | Resume S3 inventory from existing processed state. |
| `DUMMER_PROCESSED_S3_RESUME_CLUSTER_DEPTH` | `--processed-s3-resume-cluster-depth` | Path component depth used for S3 resume grouping. Defaults to `2`. |
| `DUMMER_PROCESSED_S3_MAX_RETRIES` | `--processed-s3-max-retries` | Maximum retries for public S3 list requests. Defaults to `10`. |
| `DUMMER_PROCESSED_S3_RETRY_DELAY_SECONDS` | `--processed-s3-retry-delay-seconds` | Base retry delay for public S3 list requests. Defaults to `2.0`. |

If a processed state file does not exist, Dummer treats processed state as empty.

### Path Filtering And Crawl Depth

| Environment variable | Flag | Meaning |
| --- | --- | --- |
| `DUMMER_PATH_FILTER` | `--path-filter` | Keep only directories whose relative path contains this exact component. |
| `DUMMER_PATH_FILTER_DEPTH` | `--path-filter-depth` | Optional zero-based component position for faster crawl pruning. |
| `DUMMER_CRAWL_MIN_DEPTH` | `--crawl-min-depth` | Minimum relative directory depth to include during filesystem crawl. |
| `DUMMER_CRAWL_MAX_DEPTH` | `--crawl-max-depth` | Maximum relative directory depth to include during filesystem crawl. |
| `DUMMER_SUMMARY_ANCHOR_COMPONENT` | `--summary-anchor-component` | Zero-based path component used to group reconciliation summaries. |

Depth is counted below the data/bundle directory. Root files are depth `0`;
direct child folders are depth `1`; grandchildren are depth `2`.
Hidden files and directories are ignored during crawl and manifest inventory by
default, as DUM does not upload those files. Use `--include-hidden` or `DUMMER_INCLUDE_HIDDEN=true`
to override this behavior in Dummer.

### Upload And Run Behavior

| Environment variable | Flag | Meaning |
| --- | --- | --- |
| `DUMMER_BUNDLE` | `--bundle` | Short dataset label retained for compatibility with existing reports/config. |
| `DUMMER_PREFIX` | `--prefix` | Prefix passed to DUM. If omitted, Dummer uses the parent of `--local-path`. |
| `DUMMER_CONFIG` | `--config` | DUM configuration file. |
| `DUMMER_NAME` | `--name` | DUM node/name value. |
| `DUMMER_DUM_BINARY` | `--dum-binary` | DUM executable path. |
| `DUMMER_THREADS` | `--threads` | DUM upload thread count. Defaults to `12`. |
| `DUMMER_REPORT_DIR` | `--report-dir` | DUM report directory. |
| `DUMMER_PIPELINE_REPORT_DIR` | `--pipeline-report-dir` | Dummer run report directory. |
| `DUMMER_DUM_MANIFEST_STORE` | `--dum-manifest-store` | Directory for reusable per-folder DUM checksum manifests. Incompatible with direct file-list upload. |
| `DUMMER_SCRIPT_DIR` | `--script-dir` | Base directory for default state paths and `.env` discovery. Defaults to `.`. |
| `DUMMER_LOG_LEVEL` | `--log-level` | DUM log level. Defaults to `warn`. |
| `DUMMER_INTERACTIVE` | `--interactive` | Stream DUM output through a pseudo-terminal. |
| `DUMMER_DIRECT_FILE_LIST_UPLOAD` | `--direct-file-list-upload` | Pass direct file paths to DUM instead of a folder path. |
| `DUMMER_DIRECT_FILE_LIST_BATCH_SIZE` | `--direct-file-list-batch-size` | Maximum file paths per DUM command in direct file-list mode. Defaults to `500`. |
| `DUMMER_MAX_DIRS` | `--max-dirs` | Pending folders to process in a non-loop run. Defaults to `1`. |
| `DUMMER_LOOP` | `-L`, `--loop` | Continue processing pending folders until none remain or a failure occurs. |

Use `--dum-manifest-store` only when files in already-seen folders are stable
enough for reusable checksum manifests. Use `--direct-file-list-upload` when DUM
should receive exact direct file paths rather than directory paths. These modes
are mutually exclusive.

### E2E Byte Verification

This stage is off by default. When enabled, Dummer selects complete processed
directories, downloads each selected S3 object as a stream, and compares the
remote bytes with the local file bytes. It does not use S3 checksum metadata and
does not keep downloaded files on disk.

Because every checked file is fully read back from S3, this can cost significant
time and egress. The probability setting is meant for repeated pipeline runs:
for example, a daily invocation can set `0.1` to perform verification on roughly
one in ten successful runs, while max directory/file limits keep any selected
run bounded. Use `1.0` for small datasets or temporary confidence-building runs,
and use a low value for ongoing spot checks when full egress is expensive.

| Environment variable | Flag | Meaning |
| --- | --- | --- |
| `DUMMER_INTEGRITY_CHECK` | `--integrity-check` | Enable the byte-for-byte verification stage. Defaults to off. |
| `DUMMER_INTEGRITY_RUN_PROBABILITY` | `--integrity-run-probability` | Chance that a run performs verification, from `0.0` to `1.0`. Defaults to `1.0` once enabled. |
| `DUMMER_INTEGRITY_MAX_DIRS` | `--integrity-max-dirs` | Maximum directories to verify in one run. Blank means no directory cap. |
| `DUMMER_INTEGRITY_MAX_FILES` | `--integrity-max-files` | Maximum files to download and compare in one run. Blank means no file cap. |
| `DUMMER_INTEGRITY_DIRS` | `--integrity-dirs` | Comma-separated relative directories to target. |
| `DUMMER_INTEGRITY_REPORT_DIR` | `--integrity-report-dir` | Directory for unique JSON integrity reports. Defaults to the pipeline report directory when invoked through `dummer`. |
| `DUMMER_INTEGRITY_COMPARE_CHUNK_BYTES` | `--integrity-compare-chunk-bytes` | Streaming comparison chunk size. Defaults to `1048576`. |
| `DUMMER_INTEGRITY_MAX_RETRIES` | `--integrity-max-retries` | Retry count for public S3 listing requests. Defaults to `3`. |
| `DUMMER_INTEGRITY_RETRY_DELAY_SECONDS` | `--integrity-retry-delay-seconds` | Base retry delay for public S3 listing requests. Defaults to `2.0`. |

## Utility Scripts

Most users should use `dummer setup` and `dummer`. These scripts are for manual
state building, inspection, or specialized workflows.

### `dummer_integrity.py`

Run the byte-for-byte public S3 verifier directly.

```bash
python3 dummer_integrity.py \
  --local-path /data/bundle \
  --local-state ./local_dirs.txt \
  --processed-state ./processed_s3_dirs.txt \
  --processed-s3-bucket example-public-bucket \
  --processed-s3-prefix archive/bundle/ \
  --processed-root archive/bundle \
  --integrity-dir collection/2026/day001 \
  --integrity-report-dir ./integrity-reports
```

Installed packages also provide `dummer-integrity`.

### `dummer_inventory.py`

Build local and/or processed inventory state files without uploading.

```bash
python3 dummer_inventory.py [options]
```

Common options:

| Option | Meaning |
| --- | --- |
| `--local-out PATH` | Write local inventory state here. |
| `--local-path DIR` | Crawl this local data/bundle directory. |
| `--local-manifest PATH` | Build local inventory from a plain-text or `.gz` manifest. |
| `--local-root PREFIX` | Strip this prefix from local manifest paths. |
| `--processed-out PATH` | Write processed inventory state here. |
| `--processed-crawl DIR` | Crawl this processed filesystem directory. |
| `--processed-manifest PATH` | Build processed inventory from a manifest. |
| `--processed-root PREFIX` | Strip this prefix from processed paths or S3 keys. |
| `--processed-s3-bucket BUCKET` | Public S3 bucket to list. |
| `--processed-s3-prefix PREFIX` | S3 key prefix to list. |
| `--processed-s3-region REGION` | Optional S3 region. |
| `--processed-s3-known-dirs-file PATH` | Directory list for exact-prefix S3 counts. |
| `--processed-s3-known-dirs-workers N` | Concurrent S3 exact-prefix checks. |
| `--processed-s3-resume-from-state` | Resume S3 inventory from existing processed state. |
| `--processed-s3-resume-cluster-depth N` | Path component depth used for S3 resume grouping. |
| `--processed-s3-max-retries N` | Maximum public S3 list retries. |
| `--processed-s3-retry-delay-seconds N` | Base public S3 retry delay. |
| `--path-filter`, `--path-filter-depth`, `--crawl-min-depth`, `--crawl-max-depth`, `--include-hidden` | Same behavior as the main command. |

### `dummer_reconcile.py`

Compare local and processed state files.

```bash
python3 dummer_reconcile.py --local-state ./local_dirs.txt --processed-state ./processed_dirs.txt
```

Options:

| Option | Meaning |
| --- | --- |
| `--local-state PATH` | Local state file to compare. Required. |
| `--processed-state PATH` | Processed state file to compare. Required. |
| `--summary` | Show summary counts instead of listing every pending directory. |
| `--summary-anchor-component N` | Zero-based path component used as the summary anchor. |
| `--summary-group-components SPEC` | Comma-separated component indices or labels, such as `0,1` or `collection:0,instrument:1`. |

### `dummer_upload.py`

Upload pending folders from already-built state files.

```bash
python3 dummer_upload.py --local-state ./local_dirs.txt --processed-state ./processed_dirs.txt
```

This script uses the same upload and run-behavior settings as `dummer`, and it
loads `.env` defaults.

## Examples

Run the normal guided setup:

```bash
dummer setup
dummer
```

Process all pending folders from saved setup:

```bash
dummer --loop
```

Process a limited batch:

```bash
dummer --max-dirs 5
```

Reuse checksum manifests for append-only data:

```bash
dummer --dum-manifest-store ./dum-manifests --loop
```

Use this when files in already-seen folders do not change. DUM can reuse the
stored per-folder manifests on later runs instead of repeating that checksum work.

Build state files manually:

```bash
python3 dummer_inventory.py \
  --local-out ./local_dirs.txt \
  --local-path /data/example.bundle \
  --processed-out ./processed_s3_dirs.txt \
  --processed-s3-bucket example-public-bucket \
  --processed-s3-prefix example.bundle/ \
  --processed-root example.bundle
```

Compare two state files:

```bash
python3 dummer_reconcile.py \
  --local-state ./local_dirs.txt \
  --processed-state ./processed_s3_dirs.txt
```

Upload from prepared state files:

```bash
python3 dummer_upload.py \
  --local-path /data/example.bundle \
  --local-state ./local_dirs.txt \
  --processed-state ./processed_s3_dirs.txt
```
