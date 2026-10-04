# Flink-on-K8s live run — evidence

Captured artifact, not a claim (same discipline as kube-verdict's
`docs/evidence/*-live.md` and this repo's `tools/capture_signals.py`).
Answers the last open item in the PatchTST-readiness gate kube-verdict needs
before a joint cloud deployment: did a Flink (or Dataflow) job actually run a
full window end-to-end and land a signal in the KB, on the real deployment
target — not just a unit-test fixture?

- **Date:** 2026-09-26
- **Cluster:** Rancher Desktop k3s (`lima-rancher-desktop`), `patchtst` namespace
- **Stack:** `deploy/flink` (jobmanager, 2 taskmanagers, beam-job-server) on top
  of the base `deploy/k3s` stack (Mimir/MinIO, KB)

## What was run

1. Seeded fresh synthetic metrics into Mimir (`kubectl apply -f
   deploy/k3s/40-ingest.yaml`) — `sim_cpu`/`sim_mem`, `namespace=demo,
   pod=node1`, with an injected CPU spike.
2. Submitted the pipeline to the Flink cluster via the on-demand smoke Job
   (`kubectl apply -f deploy/flink/50-submit-mimir.example.yaml`), which runs
   `runner: portable` against `beam-job-server.patchtst.svc:8099`.
3. Watched the resulting Flink job through the JobManager REST API
   (`/jobs/<id>`) to completion, then read back the written Parquet directly
   from MinIO (`s3://patchtst-kb/kb-flink`) to confirm content.

## Bug found and fixed along the way

The first live submission (job `0c037508008b8d32fee3acd8c438c936`) hung for
minutes on the `GroupByEntity -> {Detect, CountOut, Write0}` vertex, CPU
mostly idle. `/proc/<pid>/net/tcp` inside the `beam-worker-pool` container
showed a `SYN_SENT` connection to `169.254.169.254:80` — the EC2 metadata
service, unreachable from this cluster.

Root cause: `kb/store.py`'s `SignalStore._fs()` constructs a fresh
`pyarrow.fs.S3FileSystem` on every call (once per window write). Without
`AWS_EC2_METADATA_DISABLED`, the underlying AWS SDK probes IMDS for
credentials/region before falling back to the env credentials that were
already present — paying a fixed timeout (~8s, confirmed standalone with a
throwaway pod running the same image) on every single construction. Across
~90 window/entity writes for a one-hour backfill, that stalled the job
indefinitely instead of finishing in seconds.

Fix: `AWS_EC2_METADATA_DISABLED: "true"` on the `beam-worker-pool` container
(`deploy/flink/20-taskmanager.yaml`) — construction dropped from ~8s to
~0.02s. Committed in `1c07672`.

## Result after the fix

- Driver Job (`flink-submit-mimir`): `Complete`, ~15s.
- Flink job `31e4190e4216c3b5ba99895703247089`: `RUNNING` → `FINISHED` in
  ~25s (vs. hanging indefinitely before the fix).
- 90 new Parquet files landed in `s3://patchtst-kb/kb-flink` within the run.
- Sample record read back directly from MinIO:

  ```
  entity_uid=node1/demo  metric_name=sim_cpu  severity=normal  score=0.0
  method=zscore  labels={"namespace": "demo", "pod": "node1"}
  ```

  `namespace` and `pod` survived the round-trip intact — the resource-label
  contract `api/signal_mapper.py::signal_to_namespaces()` needs on the
  kube-verdict side.

## What this does and doesn't prove

**Proves:** the Flink-on-K8s deployment target is mechanically live —
submit → windowed detection → KB write, on the real cluster, with resource
labels intact through the round-trip.

**Doesn't prove:** detection sensitivity. This run's zscore detector scored
the injected anomaly `normal` (score 0.0) — the window/period alignment for
this particular smoke config didn't isolate the spike. That's a detection
calibration question, separate from the deployment-plumbing question this
document answers.

## Reruns

This session's manual sequence is now `python -m tools.live_check_flink` —
see `docs/evidence/live-checks/README.md`. Re-run it any time the Flink
manifests change, rather than re-deriving the steps by hand.

## 2026-10-04 — Flink 1.20 + Beam 2.76.0 + MinIO → seaweedfs

`apache-beam` 2.76.0 dropped the `beam_flink1.18_job_server` image (only
1.19+ now), forcing a Flink bump too, not just Beam. Separately,
`quay.io/minio/minio` now 401s for every tag (see
`docs/evidence/real-telemetry-live.md`), so the S3 checkpoint/KB-sink store
moved to seaweedfs (`deploy/flink/05-seaweedfs.yaml`) — a filesystem backend
doesn't work here the way it did for Mimir, since JobManager/TaskManager are
separate pods needing *shared* durable state and `local-path` is RWO-only.

Two real bugs found getting it green again, both now handled by
`tools/live_check_flink.py` rather than left as tribal knowledge:

- pyarrow's `S3FileSystem` refuses to auto-create a missing bucket even
  though seaweedfs' own gateway would — `allow_bucket_creation=True` has to
  be passed explicitly, client-side.
- seaweedfs' simple PUT path works with no auth at all, but its multipart
  path (what Parquet writes actually use) *does* check the access key —
  needs an explicit `-s3.config` identity (`deploy/flink/05-seaweedfs.yaml`),
  not just "no auth configured".

Result: `flink_job_state: FINISHED`, 130 Parquet files written —
`docs/evidence/live-checks/flink-20261004T145328Z.json`.
