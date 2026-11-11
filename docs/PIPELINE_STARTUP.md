# Pipeline CronJob: waiting for Mimir at start

Spec written before the change (2026-11-11).

## Observed

After a node reboot, the first `pipeline` run fails: the source's `query_range`
gets `Connection refused` from `mimir-gateway`, because Mimir's read path is not
up yet. The Job has `backoffLimit: 1` and `restartPolicy: Never`, so both
attempts fail within seconds and the tick is lost. The next tick (5 min later)
succeeds. The failed Job stays in `Failed`, which reads like a pipeline defect.

## Rule

A run must not start reading until Mimir answers a query. Waiting for Mimir is
a deployment concern, not the source connector's: `connectors/sources/mimir.py`
keeps failing fast on a real error.

## Change

`deploy/k3s/30-pipeline.yaml` gets an `initContainer` (`wait-for-mimir`) that:

- uses the pipeline image itself (no extra image to import on an air-gapped node);
- probes the same read path the source uses:
  `GET <endpoint>/prometheus/api/v1/query?query=vector(1)` must return HTTP 200
  with `"status":"success"` (gateway `/ready` alone does not prove the querier is up);
- retries every 5 s, for at most 240 s (under the 5-min schedule, so with
  `concurrencyPolicy: Forbid` a stuck wait cannot block the next tick for long);
- on timeout exits non-zero with a one-line message naming the endpoint.

The endpoint is read from the mounted `pipeline.yaml`, so it is written once.

## Checks (live, k3d)

1. Positive: with Mimir up, the init container exits 0 at once and the run
   writes a signals parquet to the KB, as before.
2. Negative: with `mimir-query-frontend` scaled to 0, the init container waits;
   scaled back up within the window, the run then completes (not `Failed`).
3. Timeout: with the read path left down, the init container exits non-zero after
   ~240 s with the message, and no pipeline container starts.

## Amendment (2026-11-11, after check 2's first attempt)

Check 2 as first written did not cut the read path: with `mimir-query-frontend`
at 0 replicas, `query=vector(1)` through the gateway still succeeded, so the init
container passed at once. The reboot failure was `Connection refused` from the
gateway itself, so checks 2 and 3 cut `mimir-gateway` (0 replicas) instead.

## Results (2026-11-11, k3d, image `patchtst-pipeline:dev`)

| Check | Outcome |
|---|---|
| 1. Positive | init exits 0 at once (`mimir ready: …`), run `Complete`, new parquet in the KB |
| 2. Negative, query-frontend at 0 | not a cut: query still succeeds (see amendment) |
| 2. Negative, gateway at 0 | pod held in `Init:0/1` for 40 s; gateway back, run `Complete` (72 s) |
| 3. Timeout, gateway at 0 | each attempt exits 1 after 240 s with `mimir not ready after 240 s at … Connection refused`; pipeline container never starts |

A scheduled tick that fell inside check 3's outage behaved the same way: its first
attempt timed out, its second waited for the gateway and completed.

Open point: with `backoffLimit: 1` a Job makes two attempts, so a read path down
for good holds the Job for ~490 s, and with `concurrencyPolicy: Forbid` the next
tick is skipped. The "under the schedule" bound holds per attempt, not per Job.

## Amendment 2 (2026-11-11): the whole Job bounded

Decision on the open point: the Job gets `activeDeadlineSeconds: 280`, which
bounds every attempt together, so it always ends before the next tick and
`Forbid` never skips one. The init wait stays at 240 s: a gateway back within the
first attempt still lets the run complete.

Checks, written before the run:

4. Positive again: with Mimir up, the run completes as in check 1.
5. Deadline: with `mimir-gateway` at 0, the Job ends `Failed` with reason
   `DeadlineExceeded` at about 280 s after its start, and the next scheduled tick
   starts on time (not skipped).

Results (2026-11-11, on the real scheduled ticks):

| Check | Outcome |
|---|---|
| 4. Positive | run `Complete`, `activeDeadlineSeconds: 280` on the Job |
| 5. Deadline, gateway at 0 | tick `16:25:00Z` ended `Failed`, `DeadlineExceeded`, at `16:30:11Z`: **311 s, not ~280 s** |
| 5. Next tick | tick `16:30` **started at 16:30:11Z**: not skipped, but 11 s late, held by `Forbid` until the previous Job ended; with the gateway back it completed |

The prediction "about 280 s, next tick on time" is half wrong: the deadline is
enforced about 30 s late, so a 280 s bound still overlaps the next tick by a few
seconds. No tick was skipped.

Decision (2026-11-11): 280 s kept. The goal was no skipped tick, and it holds; a
start a few seconds late during an outage is accepted. A lower bound (240 s job,
200 s wait) was considered and not taken.
