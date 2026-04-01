"""Smoke tests: compile each pipeline and assert valid JSON output."""

import json

from kfp.compiler import Compiler


def test_compile_clustering_pipeline(tmp_path):
    from pipeline import player_clustering_pipeline

    out = tmp_path / "clustering.json"
    Compiler().compile(
        pipeline_func=player_clustering_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "pipelineSpec" in data, "Compiled JSON missing 'pipelineSpec' key"


def test_compile_anomaly_pipeline(tmp_path):
    from anomaly_pipeline import player_anomaly_pipeline

    out = tmp_path / "anomaly.json"
    Compiler().compile(
        pipeline_func=player_anomaly_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "pipelineSpec" in data, "Compiled JSON missing 'pipelineSpec' key"


def test_compile_rating_pipeline(tmp_path):
    from rating_pipeline import player_rating_pipeline

    out = tmp_path / "rating.json"
    Compiler().compile(
        pipeline_func=player_rating_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "pipelineSpec" in data, "Compiled JSON missing 'pipelineSpec' key"
