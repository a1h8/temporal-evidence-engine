# PatchTST model demo (D1 regime-switch, real forecast + reconstruction)

Every other demo in `deploy/` (the base `deploy/k3s` CronJob, `deploy/kafka`)
runs `detector: zscore` — dependency-free, no torch. This overlay instead runs
the actual D1 mechanism: a PatchTST forecaster (NORMAL-regime anticipation)
and a self-supervised PatchTST reconstructor (INCIDENT-regime detective),
both trained on the fly per `config/pipeline.example.yaml`'s regime-switch
block.

```
ingest-seed-long Job ──remote-write──▶ Mimir ──▶ pipeline-patchtst Job ──▶ KB datalake
                                                   (regime-switch: patchtst + reconstruction)
```

**Why a longer series.** `PatchTSTDetector`/`ReconstructionDetector` need
`len(values) >= context_length + prediction_length` (72) / `>= context_length`
(64) or they silently fall back to z-score (`detection/patchtst.py`,
`detection/reconstruction.py`). The base `ingest-seed` only produces 60
points — enough for zscore, not enough to actually exercise PatchTST.
`ingest-seed-long` produces 200, 15s apart (group `node4/demo`, distinct from
`node1/demo` and the Kafka demo's `node2/demo`).

## 1. Prerequisite

Base stack applied (namespace, Mimir, `kb-datalake` PVC):

```sh
kubectl apply -k deploy/k3s
```

## 2. Build the torch image and deploy

```sh
docker build --build-arg INSTALL_TORCH=1 -t patchtst-pipeline:torch .
kubectl apply -k deploy/patchtst-model
```

`pipeline-patchtst` is a one-shot Job, not a CronJob — training two small
PatchTST models on the fly per tick is compute-heavy by design (see
`detection/patchtst.py`'s docstring: "fit for periodic/scoped assessment, not
high-frequency cluster-wide scoring").

## 3. Run the two faces (two pipeline runs)

The regime-switch detector runs **one face per call**, picked by the current
regime. A single run therefore only ever shows one face:

1. **Run 1** (applied above) starts NORMAL, so the forecast face scores the
   series' last 8 points — exactly where `ingest-seed-long` injected the CPU
   spike. It goes `critical` and the regime flips to INCIDENT.
2. **Run 2** re-reads the regime from the KB (`state: {type: kb}` in the
   ConfigMap) instead of starting NORMAL, so the reconstruction face runs.
   Re-run the pipeline only — *not* the ingest:

```sh
kubectl -n patchtst delete job pipeline-patchtst
kubectl apply -k deploy/patchtst-model
kubectl -n patchtst wait --for=condition=complete job/pipeline-patchtst --timeout=900s
```

## 4. Verify

```sh
kubectl -n patchtst port-forward svc/kb 8080:80 &
curl 'http://localhost:8080/api/v1/signals/history?entity=node4/demo'
```

For `sim_cpu`, expect run 1's signal with `method: "patchtst"`,
`labels.mode: "anticipation"`, `labels.regime: "incident"`, then run 2's
with `method: "patchtst-recon"`, `labels.mode: "detective"`. `"zscore"` would
mean a face silently fell back (too few points, or an exception logged as a
warning in the pod's output). `sim_mem` (a slow ramp, no spike) stays in the
forecast face.

## Notes / caveats

- **Resource cost.** Training two small PatchTST models (d_model=32,
  2 layers) on 200 points is CPU-bound but should complete in well under
  `activeDeadlineSeconds: 900`; the Job requests 1 CPU / 1Gi, limited to
  2 CPU / 3Gi — heavier than every other demo Job in this repo.
- **Re-seeding within the hour mixes series.** Each `ingest-seed-long` run
  re-stamps the series to end "now"; the previous run's samples are still in
  the pipeline's 1h query window, so the two copies interleave. Wait an hour
  between re-seeds, or change `node4` to a fresh pod label in both manifests.
- **Checkpoint-backed variant not covered here.** This exercises the
  train-on-the-fly path (`patchtst`/`reconstruction` detector types). The
  load-once M1 engine variants (`patchtst-infer`/`reconstruction-infer`,
  M3/M7) need pre-trained checkpoints via `inference/train_reference.py` or
  the retraining loop's `checkpoint_dir` — a separate, heavier setup.

## Teardown

```sh
kubectl delete -k deploy/patchtst-model
```
