"""Preprocess: read from BigQuery feature view, normalize, output to GCS."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=["google-cloud-bigquery", "pandas", "db-dtypes", "scikit-learn", "pyarrow"],
)
def preprocess(
    project_id: str,
    bq_view: str,
    output_dataset: dsl.Output[dsl.Dataset],
    feature_columns: dsl.Output[dsl.Artifact],
) -> int:
    """Read features from BigQuery, normalize with StandardScaler, save to GCS."""
    import json
    import pandas as pd
    from google.cloud import bigquery
    from sklearn.preprocessing import StandardScaler

    client = bigquery.Client(project=project_id)
    df = client.query(f"SELECT * FROM `{bq_view}`").to_dataframe()

    # Keep player_id for labeling, drop non-numeric columns for clustering
    id_col = "player_id"
    drop_cols = [id_col, "position", "league"]
    feature_cols = [c for c in df.columns if c not in drop_cols]

    # Fill NaN with 0 for numeric features
    features = df[feature_cols].fillna(0).astype(float)

    # Normalize
    scaler = StandardScaler()
    scaled = pd.DataFrame(scaler.fit_transform(features), columns=feature_cols)
    scaled[id_col] = df[id_col].values

    # Save
    scaled.to_parquet(output_dataset.path, index=False)

    with open(feature_columns.path, "w") as f:
        json.dump(feature_cols, f)

    print(f"Preprocessed {len(scaled)} players, {len(feature_cols)} features")
    return len(scaled)
