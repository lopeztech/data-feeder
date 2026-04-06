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
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_anomaly_pipeline(tmp_path):
    from anomaly_pipeline import player_anomaly_pipeline

    out = tmp_path / "anomaly.json"
    Compiler().compile(
        pipeline_func=player_anomaly_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_rating_pipeline(tmp_path):
    from rating_pipeline import player_rating_pipeline

    out = tmp_path / "rating.json"
    Compiler().compile(
        pipeline_func=player_rating_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_team_win_predictor_pipeline(tmp_path):
    from team_win_predictor_pipeline import team_win_predictor_pipeline

    out = tmp_path / "team_win_predictor.json"
    Compiler().compile(
        pipeline_func=team_win_predictor_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_team_archetype_pipeline(tmp_path):
    from team_archetype_pipeline import team_archetype_pipeline

    out = tmp_path / "team_archetype.json"
    Compiler().compile(
        pipeline_func=team_archetype_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_team_optimal_profile_pipeline(tmp_path):
    from team_optimal_profile_pipeline import team_optimal_profile_pipeline

    out = tmp_path / "team_optimal_profile.json"
    Compiler().compile(
        pipeline_func=team_optimal_profile_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_team_player_bridge_pipeline(tmp_path):
    from team_player_bridge_pipeline import team_player_bridge_pipeline

    out = tmp_path / "team_player_bridge.json"
    Compiler().compile(
        pipeline_func=team_player_bridge_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_nfl_team_analysis_pipeline(tmp_path):
    from nfl_team_analysis_pipeline import nfl_team_analysis_pipeline

    out = tmp_path / "nfl_team_analysis.json"
    Compiler().compile(
        pipeline_func=nfl_team_analysis_pipeline,
        package_path=str(out),
    )

    data = json.loads(out.read_text())
    assert "root" in data, "Compiled JSON missing 'root' key"


def test_compile_f1_driver_predictor_pipeline(tmp_path):
    from f1_driver_predictor_pipeline import f1_driver_predictor_pipeline

    out = tmp_path / "f1_driver_predictor.json"
    Compiler().compile(pipeline_func=f1_driver_predictor_pipeline, package_path=str(out))
    data = json.loads(out.read_text())
    assert "root" in data


def test_compile_f1_constructor_dominance_pipeline(tmp_path):
    from f1_constructor_dominance_pipeline import f1_constructor_dominance_pipeline

    out = tmp_path / "f1_constructor_dominance.json"
    Compiler().compile(pipeline_func=f1_constructor_dominance_pipeline, package_path=str(out))
    data = json.loads(out.read_text())
    assert "root" in data


def test_compile_f1_pitstop_strategy_pipeline(tmp_path):
    from f1_pitstop_strategy_pipeline import f1_pitstop_strategy_pipeline

    out = tmp_path / "f1_pitstop_strategy.json"
    Compiler().compile(pipeline_func=f1_pitstop_strategy_pipeline, package_path=str(out))
    data = json.loads(out.read_text())
    assert "root" in data
