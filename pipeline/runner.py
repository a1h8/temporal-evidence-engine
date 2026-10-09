"""Config-driven pipeline runner — the runnable entrypoint.

Assembles the cycle from config: source (SPI) → detection transform → sinks
(SPI), executed by an engine. This is what a K3s CronJob invokes.

Config shape (dict / YAML / JSON):

    source:   {type: mimir, params: {...}}
    detector: {type: regime-switch,
               forecast:  {type: patchtst},
               detective: {type: reconstruction},
               state:     {type: kb, root: ...}}   # optional, default memory
    sinks:   [{type: signal-store, params: {root: ...}}]
    engine:   {type: local}        # or beam

The ``patchtst-infer`` / ``reconstruction-infer`` detectors run the load-once M1
engine instead of training on the fly; both take ``params: {forecast_ckpt,
reconstruct_ckpt, spec}`` and, sharing those checkpoints, share one loaded engine.
Alternatively they take ``params: {checkpoint_dir: ...}`` to follow the retraining
loop's ``latest.json`` pointer (M7) — each run auto-resumes the newest retrained
checkpoint set with no config change.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import kb  # noqa: F401  (registers the signal-store sink connector)
from connectors import LocalEngine, build
from connectors.engines.base import Engine
from detection import (
    Detector,
    ForecastInferenceDetector,
    PatchTSTDetector,
    ReconstructionDetector,
    ReconstructionInferenceDetector,
    RegimeSwitchDetector,
    ZScoreDetector,
    make_detection_transform,
)
from detection.regime import InMemoryRegimeState, KBSeededRegimeState
from kb import SignalStore

_DETECTORS: dict[str, type[Detector]] = {
    "zscore": ZScoreDetector,
    "patchtst": PatchTSTDetector,
    "reconstruction": ReconstructionDetector,
    "patchtst-infer": ForecastInferenceDetector,
    "reconstruction-infer": ReconstructionInferenceDetector,
}


def build_detector(cfg: dict) -> Detector:
    """Build a Detector from config (recursive for regime-switch)."""
    kind = cfg["type"]
    if kind == "regime-switch":
        # params carry the anti-flapping knobs: enter_after / exit_after.
        kwargs = dict(cfg.get("params", {}))
        if "state" in cfg:
            kwargs["state"] = build_regime_state(cfg["state"])
        return RegimeSwitchDetector(
            forecast=build_detector(cfg["forecast"]),
            detective=build_detector(cfg["detective"]),
            **kwargs,
        )
    try:
        cls = _DETECTORS[kind]
    except KeyError:
        raise KeyError(
            f"unknown detector {kind!r}; available: "
            f"{sorted(_DETECTORS) + ['regime-switch']}"
        ) from None
    return cls(**cfg.get("params", {}))


def build_regime_state(cfg: dict) -> InMemoryRegimeState:
    """Build the regime-switch state store from config.

    ``memory`` (default) starts every key NORMAL — right for a long-lived run,
    but each batch run (CronJob tick, one-shot Job) would forget an in-flight
    incident and the detective face would never run. ``kb`` seeds each key from
    the last persisted signal's regime, so the incident carries across runs:

        state: {type: kb, root: /data/kb}
    """
    kind = cfg.get("type", "memory")
    if kind == "memory":
        return InMemoryRegimeState()
    if kind == "kb":
        return KBSeededRegimeState(SignalStore(cfg["root"]))
    raise KeyError(f"unknown regime state {kind!r}; available: ['kb', 'memory']")


def build_engine(cfg: dict | None) -> Engine:
    """Build the execution engine from config.

    ``local`` (default) is pure Python. ``beam`` runs on Apache Beam and accepts:

        engine:
          type: beam
          runner: dataflow          # direct | dataflow | flink (default direct)
          streaming: true           # unbounded windowed path (M5); default batch
          block: false              # detach after submit (streaming remote runs)
          window: {size_s: 60, period_s: 30, ...}   # WindowSpec, streaming only
          options: {project: ..., region: ..., temp_location: ...}  # runner opts
    """
    cfg = cfg or {}
    kind = cfg.get("type", "local")
    if kind == "local":
        return LocalEngine()
    if kind == "beam":
        from connectors.engines.beam import BeamEngine, WindowSpec
        from connectors.engines.runner import beam_pipeline_options

        streaming = bool(cfg.get("streaming", False))
        window = WindowSpec(**cfg["window"]) if cfg.get("window") else None
        options = beam_pipeline_options(
            cfg.get("runner", "direct"),
            streaming=streaming,
            options=cfg.get("options"),
        )
        return BeamEngine(
            pipeline_options=options,
            streaming=streaming,
            window=window,
            block=bool(cfg.get("block", True)),
        )
    raise KeyError(f"unknown engine {kind!r}; available: ['local', 'beam']")


def load_config(path: str) -> dict:
    text = Path(path).read_text()
    if path.endswith((".yaml", ".yml")):
        import yaml  # lazy: only the YAML path needs PyYAML

        return yaml.safe_load(text)
    return json.loads(text)


def run_pipeline(config: dict[str, Any], *, now_ms: int | None = None) -> None:
    """Build source/detector/sinks/engine from config and run the cycle."""
    source = build(config["source"]["type"], **config["source"].get("params", {}))
    sinks = [
        build(s["type"], **s.get("params", {})) for s in config["sinks"]
    ]
    detector = build_detector(config["detector"])
    transform = make_detection_transform(detector, now_ms=now_ms)
    engine = build_engine(config.get("engine"))
    engine.run(source, sinks, transform=transform)
