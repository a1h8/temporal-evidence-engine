"""Offline twin of the deploy/patchtst-model demo — the real D1 regime-switch
(PatchTST forecast + reconstruction, default sizes, trained on the fly) run
twice through the pipeline, with Mimir stubbed and the KB on tmp_path.

The detector block is read from the overlay's ConfigMap, so a drift in the
deployed config breaks this test instead of the cluster demo. Guarded by
torch/transformers/yaml; ~10s on CPU.
"""
import random
from pathlib import Path

import pytest

import kb  # noqa: F401  (registers signal-store)
from kb import SignalStore
from pipeline import run_pipeline

torch = pytest.importorskip("torch")
pytest.importorskip("transformers")
yaml = pytest.importorskip("yaml")

MANIFEST = Path(__file__).parent.parent / "deploy/patchtst-model/10-pipeline-patchtst.yaml"
ENTITY = "node4/demo"


def _seed_long_series():
    """Same generator as deploy/patchtst-model/00-ingest-seed-long.yaml:
    200 points, cpu ~0.2 with a spike to ~0.95 on the last 8, mem a slow ramp."""
    random.seed(11)
    cpu, mem = [], []
    for i in range(200):
        c = 0.2 + random.gauss(0, 0.02)
        if i >= 192:
            c = 0.95 + random.gauss(0, 0.01)
        cpu.append(round(c, 4))
        mem.append(round(0.4 + 0.001 * i + random.gauss(0, 0.01), 4))
    return {"sim_cpu": cpu, "sim_mem": mem}


def _mimir_result():
    return [
        {
            "metric": {"__name__": name, "pod": "node4", "namespace": "demo"},
            "values": [[i * 15, str(v)] for i, v in enumerate(values)],
        }
        for name, values in _seed_long_series().items()
    ]


def _demo_config(root):
    """The overlay's pipeline.yaml, with the KB paths pointed at tmp_path."""
    docs = list(yaml.safe_load_all(MANIFEST.read_text()))
    cm = next(d for d in docs if d["kind"] == "ConfigMap")
    cfg = yaml.safe_load(cm["data"]["pipeline.yaml"])
    cfg["source"]["params"].update(start=0, end=3000)
    cfg["detector"]["state"]["root"] = root
    cfg["sinks"] = [{"type": "signal-store", "params": {"root": root}}]
    return cfg


def test_demo_config_is_the_real_d1_regime_switch(tmp_path):
    det = _demo_config(str(tmp_path))["detector"]
    assert det["type"] == "regime-switch"
    assert det["forecast"]["type"] == "patchtst"
    assert det["detective"]["type"] == "reconstruction"
    # without kb state each Job run starts NORMAL and run 2 never goes detective
    assert det["state"]["type"] == "kb"


def test_demo_two_runs_show_both_faces(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "connectors.sources.mimir.query_range", lambda *a, **k: _mimir_result()
    )
    torch.manual_seed(0)
    root = str(tmp_path / "kb")
    cfg = _demo_config(root)

    # two separate run_pipeline calls = two Job runs: fresh detector, shared KB
    run_pipeline(cfg, now_ms=1_700_000_001_000)
    run_pipeline(cfg, now_ms=1_700_000_002_000)

    signals = SignalStore(root).query(ENTITY)
    cpu = sorted((s for s in signals if s.metric_name == "sim_cpu"), key=lambda s: s.ts)
    mem = sorted((s for s in signals if s.metric_name == "sim_mem"), key=lambda s: s.ts)
    assert len(cpu) == 2 and len(mem) == 2

    # run 1: the forecast face itself (not a z-score fallback) flags the spike.
    # The score is the forecaster's own residual ratio — the level-shift check
    # can only raise the severity, so score >= critical proves the model saw it.
    run1 = cpu[0]
    assert run1.method == "patchtst" and run1.n_points == 200
    assert run1.severity == "critical" and run1.score >= 3.0
    assert run1.labels["mode"] == "anticipation" and run1.labels["regime"] == "incident"

    # run 2: the regime is resumed from the KB, so the reconstruction face runs.
    # Its severity depends on the training seed (critical or warning); the
    # incident holds either way (the plateau's level shift blocks recovery).
    run2 = cpu[1]
    assert run2.method == "patchtst-recon"
    assert run2.labels["mode"] == "detective" and run2.labels["regime"] == "incident"

    # the slow mem ramp never breaks: forecast face on both runs, stays NORMAL
    assert [s.method for s in mem] == ["patchtst", "patchtst"]
    assert all(s.labels["regime"] == "normal" for s in mem)
