# dummer

dummer is an operations wrapper around DUM for reliable, interruptible, state-aware
uploads. It answers the practical question admins face during large uploads: "Given the
files I have locally and what appears to have already been processed, where does DUM
still need to run?"

The tool builds or reuses a cheap local inventory, compares it against processed state,
runs DUM only for directories that are not present or incomplete in processed state, and
records each successful upload back into state. That makes repeated runs efficient for
file sets that are unchanged or only growing: dummer can count files and reconcile state
instead of forcing DUM to checksum and process the entire filesystem tree again.

The result is not just reconciliation, but a safer operating loop. Long uploads can be
interrupted and resumed, completed work is skipped, failures retry only the affected
directories, and every run leaves behind state and reports that can be inspected for
verification.

dummer keeps the upload workflow inspectable. Inventory state is a simple tab-separated
file, reconciliation drift is reported, DUM JSON reports are preserved, and pipeline run
reports capture what happened for each attempted directory. Long-running crawls, S3
inventory builds, and non-interactive uploads emit timestamped progress messages so quiet
runs are easier to monitor.

Both the local side and the processed side use the same state-file format:

`relative/directory/path\t<number_of_files>`

Local inventory can be crawled from the real file root, parsed from a manifest, or loaded
from a prebuilt state file. Processed state can come from an existing state file, another
manifest, a filesystem crawl, or an anonymous listing of a public AWS S3 bucket.

## Configuration

The job dummer is built around is:

1. Inventory local files under a filesystem root.
2. Reconcile that inventory with what has already been processed.
3. Upload the missing directories.
4. Record successful uploads back into processed state.

The most important setting is therefore the local file root:

```bash
--local-path /dsk8/catalina/gbo.ast.catalina.survey
```

or:

```bash
DUMMER_LOCAL_PATH=/dsk8/catalina/gbo.ast.catalina.survey
```

If you do not provide a prebuilt local inventory, dummer crawls `--local-path`. If you
already did inventory work ahead of time, pass `--local-state` or `--local-manifest` to
skip that crawl. Uploads still use `--local-path` as the root for the actual files.

`dummer.py` resolves settings in this order:

1. Explicit CLI arguments
2. Environment variables such as `DUMMER_LOCAL_PATH` and `DUMMER_REPORT_DIR`
3. A `.env` file in the current directory or `--script-dir`
4. Built-in defaults for non-environment-specific settings only

Environment-specific values like the local file root, DUM config path, and report
directories are not baked into the code. The DUM executable defaults to
`/usr/local/bin/pds-ingress-client`, and can be changed with `--dum-binary` or
`DUMMER_DUM_BINARY`. Copy `.env.example` to `.env` and adjust it for the target
environment, or set the same `DUMMER_*` variables in the shell.

Not every script reads environment defaults today. `dummer.py` and `dummer_upload.py`
resolve settings from CLI, environment, `.env`, then built-in defaults. `dummer_inventory.py`
and `dummer_reconcile.py` are explicit-flag utilities, so pass their arguments on the
command line unless noted otherwise.

### Primary Settings

| Environment variable | CLI flag | Meaning |
| --- | --- | --- |
| `DUMMER_LOCAL_PATH` | `--local-path` | Root directory containing the local files dummer may inventory and upload. This is the primary path admins usually set. |
| `DUMMER_PROCESSED_STATE` | `--processed-state` | Reuse an existing processed dir/count state file. Exclusive with processed manifest/crawl/S3 sources in `dummer.py`. |
| `DUMMER_PROCESSED_S3_BUCKET` | `--processed-s3-bucket` | Public S3 bucket to inspect for processed state. |
| `DUMMER_PROCESSED_S3_PREFIX` | `--processed-s3-prefix` | S3 key prefix to list. |
| `DUMMER_PATH_FILTER` | `--path-filter` | Track and upload only directories whose relative path contains this exact component. |
| `DUMMER_CONFIG` | `--config` | DUM config file passed with `-c`. |
| `DUMMER_NAME` | `--name` | DUM Node name parameter passed with `-n`. |
| `DUMMER_DUM_BINARY` | `--dum-binary` | DUM executable path. Defaults to `/usr/local/bin/pds-ingress-client`. |
| `DUMMER_THREADS` | `--threads` | DUM upload thread count. |
| `DUMMER_REPORT_DIR` | `--report-dir` | Directory for DUM JSON reports. Required when upload work exists. |
| `DUMMER_PIPELINE_REPORT_DIR` | `--pipeline-report-dir` | Directory for dummer pipeline run reports. Required when upload work exists. |
| `DUMMER_SCRIPT_DIR` | `--script-dir` | Base directory for default state file paths and `.env` discovery. |
| `DUMMER_MAX_DIRS` | `--max-dirs` | Number of pending directories to upload in a non-loop run. |
| `DUMMER_LOOP` | `--loop` | Continue uploading until failure or exhaustion. Boolean. |

### Optional Efficiency Overrides

These options are for skipping work that has already been done, or for tuning large
inventory runs. They are not the normal starting point.

| Environment variable | CLI flag | Meaning |
| --- | --- | --- |
| `DUMMER_LOCAL_STATE` | `--local-state` | Reuse an existing local dir/count state file instead of crawling or parsing local inventory. |
| `DUMMER_LOCAL_MANIFEST` | `--local-manifest` | Build local inventory from a plain-text or `.gz` file manifest instead of crawling `--local-path`. |
| `DUMMER_LOCAL_ROOT` | `--local-root` | Leading manifest path prefix to strip before counting local parent directories. |
| `DUMMER_PROCESSED_MANIFEST` | `--processed-manifest` | Build processed state from a manifest instead of using processed state/S3. |
| `DUMMER_PROCESSED_CRAWL` | `--processed-crawl` | Crawl a filesystem path to build processed inventory. |
| `DUMMER_PROCESSED_ROOT` | `--processed-root` | Leading processed manifest or S3 key prefix to strip before counting directories. |
| `DUMMER_PROCESSED_S3_REGION` | `--processed-s3-region` | Optional S3 bucket region. |
| `DUMMER_PROCESSED_S3_KNOWN_DIRS_FILE` | `--processed-s3-known-dirs-file` | Known directory list for exact-prefix S3 counting. |
| `DUMMER_PROCESSED_S3_KNOWN_DIRS_WORKERS` | `--processed-s3-known-dirs-workers` | Worker count for known-directory S3 counting. |
| `DUMMER_PROCESSED_S3_RESUME_FROM_STATE` | `--processed-s3-resume-from-state` | Resume S3 inventory from an existing processed state file. Boolean. |
| `DUMMER_PROCESSED_S3_RESUME_CLUSTER_DEPTH` | `--processed-s3-resume-cluster-depth` | Path component depth used as the S3 resume cluster boundary. |
| `DUMMER_PROCESSED_S3_MAX_RETRIES` | `--processed-s3-max-retries` | Max retries for public S3 list requests. |
| `DUMMER_PROCESSED_S3_RETRY_DELAY_SECONDS` | `--processed-s3-retry-delay-seconds` | Base retry delay for public S3 list requests. |
| `DUMMER_PATH_FILTER_DEPTH` | `--path-filter-depth` | Optional crawl-only pruning hint: depth where the path filter is expected. |
| `DUMMER_CRAWL_MIN_DEPTH` | `--crawl-min-depth` | Optional minimum relative directory depth for filesystem crawl inventory. |
| `DUMMER_CRAWL_MAX_DEPTH` | `--crawl-max-depth` | Optional maximum relative directory depth for filesystem crawl inventory. |
| `DUMMER_SUMMARY_ANCHOR_COMPONENT` | `--summary-anchor-component` | Path component index used as the reconciliation summary anchor. |
| `DUMMER_PREFIX` | `--prefix` | Override the DUM `--prefix` value. If omitted, dummer uses the parent of `--local-path`. |
| `DUMMER_BUNDLE` | `--bundle` | Optional label retained for compatibility with existing reports/config; upload paths are rooted at `--local-path`. |
| `DUMMER_LOG_LEVEL` | `--log-level` | DUM log level. |
| `DUMMER_INTERACTIVE` | `--interactive` | Stream DUM output through a pseudo-terminal. Boolean. |

Booleans accept `1/0`, `true/false`, `yes/no`, or `on/off`.

## Scripts

- `./dummer.py`: orchestrate the full flow. Build local state, build processed state when needed, reconcile, then upload pending directories.
- `./dummer_inventory.py`: build local state, processed state, or both in one run.
- `./dummer_reconcile.py`: compare two state files and print the mismatches.
- `./dummer_upload.py`: upload pending directories based on a local-state file and a processed-state file.

## Default Flow

1. Resolve local inventory from `--local-state`, `--local-manifest`, or by crawling `--local-path`.
2. Choose exactly one processed source: existing state file, manifest, crawl path, or public S3 bucket.
3. Reconcile local state against processed state.
4. Upload any pending directories and compactly update the processed state file in use.

Manifest, crawl, and S3 sources always rebuild their intermediate state file before
reconcile. Existing state-file sources are reused as-is. Local source precedence is:
`--local-state`, then `--local-manifest`, then crawl `--local-path`. This lets admins keep
the real local file path configured for upload while still skipping local inventory work
when they already have a state file or manifest.

If a `.env` file sets a processed source but you pass a different processed source on the
command line, the CLI source wins for that side. If multiple processed sources are set
within the same source layer, dummer exits with a clear error instead of guessing.

If the processed state file is missing and you use `--processed-state`, the app does
not fail. It treats processed state as empty, so every local directory is considered
pending until uploads succeed and write entries.

## Inventory Sources

Local inventory sources:

- `--local-state`: reuse a dir/count state file.
- `--local-manifest`: read a plain-text or gzipped manifest and count files by parent directory.
- `--local-path`: walk this filesystem root and count direct files when no local state or manifest is provided.

Supplying both `--local-state` and `--local-manifest` is rejected because those are two
prepared inventory sources for the same side.

Processed inventory sources:

- `--processed-state`: reuse a dir/count state file.
- `--processed-manifest`: build processed state from a manifest.
- `--processed-crawl`: walk a filesystem path and count direct files.
- `--processed-s3-bucket`: build processed state by anonymously listing a public bucket with `ListObjectsV2`.

Public S3 mode only works if the bucket grants anonymous `ListBucket` access. Public
object reads alone are not enough. Folder-marker keys ending in `/` are ignored.
S3 inventory builds also support request retries plus resume-from-state behavior with
`--processed-s3-resume-from-state`. Resume trims the existing processed state back to the
last path-component cluster, trusts earlier clusters, and restarts listing at that
cluster boundary. The default cluster depth is `2` to preserve existing behavior, and can
be changed with `--processed-s3-resume-cluster-depth`. This resume mode requires
`--processed-root` so the tool can reconstruct the full S3 `start-after` key.

If you already have a known-directory list, S3 mode can also count those directories with
exact-prefix requests instead of crawling the whole prefix. Use
`--processed-s3-known-dirs-file` plus `--processed-s3-known-dirs-workers` to fan out the
directory counts concurrently. Each exact-prefix request uses S3 delimiter mode so only
direct child objects are counted for that directory, even when deeper descendants exist.
In this mode, resume-from-state trusts any existing counts already present in the output
state file and skips those directories on the next run.

When `--path-filter` is provided, inventory and upload steps only keep directories whose
relative path contains that exact component. Crawl mode does not prune by default. If the
filter is known to live at a specific path depth, `--path-filter-depth N` enables generic
sibling pruning as a performance hint.

Filesystem crawl inventory can also be limited by relative directory depth. Direct files
under the crawl root are counted under the `.` state key at depth `0`. Direct child
directories under the crawl root are depth `1`, grandchildren are depth `2`, and so on.
Use `--crawl-max-depth 1` to inventory root files and direct child directories without
descending into grandchildren. Use `--crawl-max-depth 0` to inventory only root files.
`--crawl-min-depth` skips shallower directories while still walking deeper ones until
`--crawl-max-depth` is reached.

## Examples

Build both state files in one run:

```bash
./dummer_inventory.py \
  --path-filter 2020 \
  --path-filter-depth 2 \
  --local-out ./local_dirs.txt \
  --local-manifest /path/to/files.txt.gz \
  --local-root gbo.ast.catalina.survey \
  --processed-out ./processed_s3_dirs.txt \
  --processed-s3-bucket example-public-bucket \
  --processed-s3-prefix gbo.ast.catalina.survey/ \
  --processed-root gbo.ast.catalina.survey
```

Build processed state from a known directory list with parallel exact-prefix S3 requests:

```bash
./dummer_inventory.py \
  --processed-out ./processed_s3_dirs.txt \
  --processed-s3-bucket example-public-bucket \
  --processed-s3-prefix sbn/gbo.ast.catalina.survey/ \
  --processed-root sbn/gbo.ast.catalina.survey \
  --processed-s3-known-dirs-file /path/to/dir_list.latest.txt \
  --processed-s3-known-dirs-workers 10
```

Filter by any path component, not just numeric/date-like values:

```bash
./dummer_inventory.py \
  --path-filter quicklook \
  --local-out ./quicklook_local_dirs.txt \
  --local-path /data/example.bundle
```

Reconcile the two state files:

```bash
./dummer_reconcile.py \
  --local-state ./local_dirs.txt \
  --processed-state ./processed_s3_dirs.txt
```

Show summary totals anchored and grouped by path components:

```bash
./dummer_reconcile.py \
  --local-state ./local_dirs.txt \
  --processed-state ./processed_s3_dirs.txt \
  --summary \
  --summary-anchor-component 2 \
  --summary-group-components collection:0,instrument:1
```

Upload from those state files:

```bash
./dummer_upload.py \
  --local-path /dsk8/catalina/gbo.ast.catalina.survey \
  --local-state ./local_dirs.txt \
  --processed-state ./processed_s3_dirs.txt \
  --loop
```

Run upload with live interactive DUM output:

```bash
./dummer_upload.py \
  --local-path /dsk8/catalina/gbo.ast.catalina.survey \
  --local-state ./local_dirs.txt \
  --processed-state ./processed_s3_dirs.txt \
  --interactive
```

Run the full sequential flow in one command:

```bash
./dummer.py \
  --local-path /dsk8/catalina/gbo.ast.catalina.survey \
  --local-manifest /path/to/files.txt.gz \
  --local-root gbo.ast.catalina.survey \
  --processed-s3-bucket example-public-bucket \
  --processed-s3-prefix gbo.ast.catalina.survey/ \
  --processed-root gbo.ast.catalina.survey \
  --path-filter 2020 \
  --path-filter-depth 2 \
  --summary-anchor-component 2 \
  --loop
```

Run the full flow by crawling the local path directly:

```bash
./dummer.py \
  --local-path /dsk8/catalina/gbo.ast.catalina.survey \
  --processed-state ./processed_dirs.txt \
  --loop
```
