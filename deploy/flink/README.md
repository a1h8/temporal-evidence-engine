# Flink-on-K8s (self-hosted portable runner) — M6

The self-hosted alternative to Dataflow: run the **same** streaming pipeline on
an Apache Flink cluster on your own Kubernetes (k3s). Dataflow-first is the
agreed M6 order (managed, least ops); this is the "own your runner" path, on the
existing `patchtst` namespace.

## How Beam runs on Flink

Flink is JVM; the pipeline is Python. The Beam **portability** layer bridges them:

```
python -m pipeline (runner: portable)
        │  Beam graph over gRPC
        ▼
  Beam Job Server  ──translates──▶  Flink JobManager
  (beam_flink1.20_    submits a        │ schedules
   job_server:2.76.0) native job       ▼
                                   Flink TaskManagers
                                     └─ SDK harness sidecar (Python)  ← runs the detector
                                        via the Fn API (localhost:50000)
```

- **JobManager / TaskManagers** — the Flink cluster (`10-`, `20-`).
- **Beam Job Server** (`30-`) — turns the Beam graph into a Flink job; the driver
  submits here (`job_endpoint`), never to Flink directly.
- **SDK harness sidecar** — each TaskManager pod runs `beam_python3.11_sdk` in
  `--worker_pool` mode so Python DoFns execute in-pod (`environment_type=EXTERNAL`).

Everything is version-locked: Flink **1.20**, Beam **2.76.0** (apache-beam
2.76 dropped the `beam_flink1.18_job_server` image, forcing this bump — see
`requirements-connectors.txt`'s `apache-beam>=2.70` floor). Bump all three
tags (Flink images, `beam_flink1.20_job_server`, `beam_python3.11_sdk`) together.

## Deploy

Requires the base stack (namespace + Mimir) already applied (`deploy/k3s`).

```sh
# seaweedfs (05-seaweedfs.yaml) runs its S3 gateway with no auth -- these
# values are placeholders Hadoop's S3A client requires non-empty but
# seaweedfs never checks. Only use real secret values here if you swap the
# object store for one that actually enforces credentials.
kubectl -n patchtst create secret generic flink-s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID=placeholder \
  --from-literal=AWS_SECRET_ACCESS_KEY=placeholder \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -k deploy/flink
kubectl -n patchtst rollout status deploy/flink-jobmanager
```

The `patchtst-flink` bucket is created on first checkpoint write — seaweedfs'
S3 gateway auto-creates buckets on upload (`-s3.autoCreateBucket`, on by
default), unlike MinIO.

## Submit

From a pod/host that can reach `beam-job-server.patchtst.svc:8099`:

```sh
python -m pipeline config/flink-streaming.example.yaml
```

`runner: portable` + `job_endpoint` sends the graph to the job server, which
submits the native Flink job. Watch it in the Flink UI:

```sh
kubectl -n patchtst port-forward svc/flink-jobmanager 8081:8081
# http://localhost:8081 — running job, checkpoints, watermark, back-pressure
```

## What you get over Dataflow / DirectRunner

- **Exactly-once + durable checkpoints** (RocksDB → S3/seaweedfs): a crash resumes
  in-flight windows instead of losing them.
- **Sovereignty**: everything on your cluster, nothing on GCP — consistent with
  the knowledge-base role feeding kube-verdict.

## Cost

You operate JobManager, TaskManagers, the Job Server, and the checkpoint store,
and you own version alignment and back-pressure/slot tuning. That's the trade
against Dataflow's zero-ops.

## Teardown

```sh
kubectl delete -k deploy/flink
```
