"""Team preprocess: read from BigQuery feature view, normalize, output to GCS."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-bigquery==3.30.0", "pandas==2.2.3", "db-dtypes==1.3.1",
        "scikit-learn==1.6.1", "pyarrow==18.1.0", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def team_preprocess(
    project_id: str,
    bq_view: str,
    output_dataset: dsl.Output[dsl.Dataset],
    raw_dataset: dsl.Output[dsl.Dataset],
    feature_columns: dsl.Output[dsl.Artifact],
) -> int:
    """Read team features from BigQuery, normalize with StandardScaler, save to GCS.

    Outputs:
    - output_dataset: StandardScaler-normalized features + year + team columns
    - raw_dataset: original un-scaled features + year + team columns
    - feature_columns: JSON list of feature column names (excluding year, team, is_playoff)
    """
    import json
    import pandas as pd
    from google.cloud import bigquery
    from sklearn.preprocessing import StandardScaler

    client = bigquery.Client(project=project_id)
    df = client.query(f"SELECT * FROM `{bq_view}`").to_dataframe()

    # Keep year and team for labeling, drop non-feature columns for clustering
    id_cols = ["year", "team"]
    drop_cols = id_cols + ["is_playoff"]
    feature_cols = [c for c in df.columns if c not in drop_cols]

    # Fill NaN with 0 for numeric features
    features = df[feature_cols].fillna(0).astype(float)

    # Save raw dataset with identifiers
    raw_df = features.copy()
    raw_df["year"] = df["year"].values
    raw_df["team"] = df["team"].values
    raw_df.to_parquet(raw_dataset.path, index=False)

    # Normalize
    scaler = StandardScaler()
    scaled = pd.DataFrame(scaler.fit_transform(features), columns=feature_cols)
    scaled["year"] = df["year"].values
    scaled["team"] = df["team"].values

    # Save
    scaled.to_parquet(output_dataset.path, index=False)

    with open(feature_columns.path, "w") as f:
        json.dump(feature_cols, f)

    print(f"Preprocessed {len(scaled)} team-seasons, {len(feature_cols)} features")
    return len(scaled)
