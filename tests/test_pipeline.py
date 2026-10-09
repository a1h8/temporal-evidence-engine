"""Pipeline runner tests — config-driven build + end-to-end run + CLI."""
import json

import pytest

import kb  # noqa: F401  (registers signal-store)
from detection import (
    PatchTSTDetector,
    ReconstructionDetector,
    RegimeSwitchDetector,
    ZScoreDetector,
)
from kb import SignalStore
from pipeline import build_detector, build_engine, load_config, run_pipeline

STABLE_THEN_SPIKE = [10.0] * 50 + [200.0]


def _mimir_result(values):
    return [
        {
            "metric": {"__name__": "cpu", "instance": "pod-a"},
            "values": [[i * 15, str(v)] for i, v in enumerate(values)],
        }
    ]


def _config(root):
    return {
        "source": {
            "type": "mimir",
            "params": {"endpoint": "http://m", "promql": "cpu", "start": 0, "end": 999, "step_s": 15},
        },
        "detector": {"type": "zscore", "params": {"min_points": 5}},
        "sinks": [{"type": "signal-store", "params": {"root": root}}],
        "engine": {"type": "local"},
    }


# --- detector factory -----------------------------------------------------

def test_build_detector_simple_types():
    assert isinstance(build_detector({"type": "zscore"}), ZScoreDetector)
    assert isinstance(build_detector({"type": "patchtst"}), PatchTSTDetector)
    assert isinstance(build_detector({"type": "reconstruction"}), ReconstructionDetector)


def test_build_detector_regime_switch_nested():
    d = build_detector({
        "type": "regime-switch",
        "forecast": {"type": "patchtst"},
        "detective": {"type": "reconstruction"},
    })
    assert isinstance(d, RegimeSwitchDetector)
    assert isinstance(d.forecast, PatchTSTDetector)
    assert isinstance(d.detective, ReconstructionDetector)
    assert d.enter_after == 1 and d.exit_after == 1   # defaults


def test_build_detector_regime_switch_forwards_anti_flapping_params():
    d = build_detector({
        "type": "regime-switch",
        "forecast": {"type": "patchtst"},
        "detective": {"type": "reconstruction"},
        "params": {"enter_after": 3, "exit_after": 2},
    })
    assert d.enter_after == 3 and d.exit_after == 2


def test_build_detector_regime_switch_defaults_to_in_memory_state():
    from detection.regime import InMemoryRegimeState, KBSeededRegimeState

    d = build_detector({
        "type": "regime-switch",
        "forecast": {"type": "zscore"},
        "detective": {"type": "zscore"},
    })
    assert type(d.state) is InMemoryRegimeState
    assert not isinstance(d.state, KBSeededRegimeState)


def test_build_detector_regime_switch_kb_state_resumes_incident_across_runs(tmp_path):
    from kb.signal import SignalRecord

    root = str(tmp_path / "kb")
    SignalStore(root).write([SignalRecord("pod-a", "cpu", 1, "critical", score=4.0,
                                          method="patchtst", labels={"regime": "incident"})])
    cfg = {
        "type": "regime-switch",
        "forecast": {"type": "zscore", "params": {"min_points": 5}},
        "detective": {"type": "zscore", "params": {"min_points": 5}},
        "state": {"type": "kb", "root": root},
    }
    # a fresh build = a fresh batch run: the regime comes from the KB, not NORMAL
    sig = build_detector(cfg).detect("pod-a", "cpu", STABLE_THEN_SPIKE, ts=2, labels=None)
    assert sig.labels["mode"] == "detective"


def test_build_detector_regime_switch_unknown_state_raises():
    with pytest.raises(KeyError, match="unknown regime state"):
        build_detector({
            "type": "regime-switch",
            "forecast": {"type": "zscore"},
            "detective": {"type": "zscore"},
            "state": {"type": "nope"},
        })


def test_build_detector_unknown_raises():
    with pytest.raises(KeyError, match="unknown detector"):
        build_detector({"type": "nope"})


# --- engine factory -------------------------------------------------------

def test_build_engine_local_default():
    from connectors import LocalEngine

    assert isinstance(build_engine(None), LocalEngine)
    assert isinstance(build_engine({"type": "local"}), LocalEngine)


def test_build_engine_beam():
    pytest.importorskip("apache_beam")
    from connectors.engines.beam import BeamEngine

    assert isinstance(build_engine({"type": "beam"}), BeamEngine)


def test_build_engine_unknown_raises():
    with pytest.raises(KeyError, match="unknown engine"):
        build_engine({"type": "nope"})


# --- config loading -------------------------------------------------------

def test_load_config_json(tmp_path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps({"a": 1}))
    assert load_config(str(p)) == {"a": 1}


def test_load_config_yaml(tmp_path):
    pytest.importorskip("yaml")
    p = tmp_path / "c.yaml"
    p.write_text("a: 1\nb: [x, y]\n")
    assert load_config(str(p)) == {"a": 1, "b": ["x", "y"]}


# --- end-to-end -----------------------------------------------------------

def test_run_pipeline_end_to_end(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "connectors.sources.mimir.query_range",
        lambda *a, **k: _mimir_result(STABLE_THEN_SPIKE),
    )
    root = str(tmp_path / "kb")
    run_pipeline(_config(root), now_ms=1700000000000)

    signals = SignalStore(root).query("pod-a")
    assert len(signals) == 1
    assert signals[0].severity == "critical" and signals[0].ts == 1700000000000


def test_cli_main(monkeypatch, tmp_path):
    from pipeline.__main__ import main

    monkeypatch.setattr(
        "connectors.sources.mimir.query_range",
        lambda *a, **k: _mimir_result(STABLE_THEN_SPIKE),
    )
    root = str(tmp_path / "kb")
    cfg_path = tmp_path / "pipeline.json"
    cfg_path.write_text(json.dumps(_config(root)))

    assert main([str(cfg_path)]) == 0
    assert len(SignalStore(root).query("pod-a")) == 1
