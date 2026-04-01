"""Unit tests for KMeans training logic (k sweep + best model selection)."""

import numpy as np
from sklearn.cluster import KMeans
from sklearn.datasets import make_blobs
from sklearn.metrics import silhouette_score


def _sweep_kmeans(X: np.ndarray, min_k: int, max_k: int):
    """Replicate the core k-sweep logic from the train KFP component."""
    best_k = min_k
    best_score = -1.0
    best_model = None

    for k in range(min_k, max_k + 1):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)

        if score > best_score:
            best_score = score
            best_k = k
            best_model = km

    # Retrain with best k (mirrors pipeline behaviour)
    final = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    final.fit(X)
    return final, best_k, best_score


def test_kmeans_sweep():
    X, _ = make_blobs(n_samples=100, n_features=5, centers=3, random_state=0)

    model, best_k, best_score = _sweep_kmeans(X, min_k=2, max_k=5)

    assert 2 <= best_k <= 5, f"best_k={best_k} out of range"
    assert best_score > 0, f"silhouette={best_score} should be positive"
    assert hasattr(model, "cluster_centers_"), "model missing cluster_centers_"
    assert model.cluster_centers_.shape == (best_k, 5)
