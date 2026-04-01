"""Unit tests for preprocess logic (StandardScaler normalisation)."""

import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import StandardScaler


@pytest.fixture()
def raw_dataframe():
    """Synthetic player data mirroring the BigQuery feature view schema."""
    rng = np.random.default_rng(42)
    n = 10
    return pd.DataFrame(
        {
            "player_id": range(1, n + 1),
            "position": rng.choice(["FW", "MF", "DF", "GK"], size=n),
            "league": rng.choice(["EPL", "La Liga", "Serie A"], size=n),
            "goals": rng.integers(0, 30, size=n).astype(float),
            "assists": rng.integers(0, 20, size=n).astype(float),
            "tackles": rng.integers(0, 100, size=n).astype(float),
            "saves": rng.integers(0, 50, size=n).astype(float),
            "rating": rng.uniform(5.0, 9.0, size=n),
            "appearances": rng.integers(1, 40, size=n).astype(float),
            "minutes_played": rng.integers(90, 3500, size=n).astype(float),
        }
    )


def _run_preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Replicate the core logic inside the preprocess KFP component."""
    id_col = "player_id"
    drop_cols = [id_col, "position", "league"]
    feature_cols = [c for c in df.columns if c not in drop_cols]

    features = df[feature_cols].fillna(0).astype(float)

    scaler = StandardScaler()
    scaled = pd.DataFrame(scaler.fit_transform(features), columns=feature_cols)
    scaled[id_col] = df[id_col].values

    return scaled, feature_cols


def test_output_shape(raw_dataframe):
    scaled, feature_cols = _run_preprocess(raw_dataframe)
    # Same number of rows; feature columns + player_id
    assert scaled.shape == (len(raw_dataframe), len(feature_cols) + 1)


def test_player_id_preserved(raw_dataframe):
    scaled, _ = _run_preprocess(raw_dataframe)
    assert "player_id" in scaled.columns
    assert list(scaled["player_id"]) == list(raw_dataframe["player_id"])


def test_non_numeric_columns_excluded(raw_dataframe):
    _, feature_cols = _run_preprocess(raw_dataframe)
    assert "player_id" not in feature_cols
    assert "position" not in feature_cols
    assert "league" not in feature_cols


def test_features_normalised(raw_dataframe):
    scaled, feature_cols = _run_preprocess(raw_dataframe)
    for col in feature_cols:
        assert abs(scaled[col].mean()) < 1e-6, f"{col} mean not ~0"
        assert abs(scaled[col].std(ddof=0) - 1.0) < 1e-6, f"{col} std not ~1"
