"""Detect anomalies: train Isolation Forest, score players, write to BigQuery."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def detect_anomalies(
    dataset: dsl.Input[dsl.Dataset],
    feature_columns: dsl.Input[dsl.Artifact],
    project_id: str,
    region: str,
    bq_dataset: str,
    contamination: float,
    report: dsl.Output[dsl.Artifact],
):
    """Train Isolation Forest, score all players, write player_anomalies to BigQuery."""
    import json
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import IsolationForest
    from google.cloud import bigquery

    df = pd.read_parquet(dataset.path)

    with open(feature_columns.path) as f:
        feature_cols = json.load(f)

    X = df[feature_cols].values

    # Train Isolation Forest
    iso = IsolationForest(
        contamination=contamination,
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(X)

    # Score: decision_function returns signed distance (negative = anomaly)
    raw_scores = iso.decision_function(X)
    predictions = iso.predict(X)  # 1 = normal, -1 = anomaly

    # Normalize score to [0, 1] where higher = more anomalous
    anomaly_scores = np.round(1 / (1 + np.exp(raw_scores)), 4)  # sigmoid inversion
    is_anomaly = (predictions == -1)

    n_anomalies = int(is_anomaly.sum())
    print(f"Detected {n_anomalies} anomalies out of {len(df)} players ({100*n_anomalies/len(df):.1f}%)")

    # Analyze top anomalies: which features deviate most
    anomaly_df = df[is_anomaly].copy()
    global_means = df[feature_cols].mean()
    top_deviations = []
    for _, row in anomaly_df.iterrows():
        devs = {col: round(float(abs(row[col] - global_means[col])), 3) for col in feature_cols}
        sorted_devs = sorted(devs.items(), key=lambda x: x[1], reverse=True)[:3]
        top_deviations.append([f[0] for f in sorted_devs])

    # Write to BigQuery
    output_df = pd.DataFrame({
        "player_id": df["player_id"].astype(str),
        "anomaly_score": anomaly_scores,
        "is_anomaly": is_anomaly.astype(int),
    })

    client = bigquery.Client(project=project_id, location=region)
    table_id = f"{project_id}.{bq_dataset}.player_anomalies"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("player_id", "STRING"),
            bigquery.SchemaField("anomaly_score", "FLOAT64"),
            bigquery.SchemaField("is_anomaly", "INT64"),
        ],
    )
    client.load_table_from_dataframe(output_df, table_id, job_config=job_config).result()
    print(f"Wrote {len(output_df)} rows to {table_id}")

    # Report
    report_data = {
        "total_players": len(df),
        "n_anomalies": n_anomalies,
        "contamination": contamination,
        "anomaly_rate": round(n_anomalies / len(df), 4),
        "score_stats": {
            "mean": round(float(anomaly_scores.mean()), 4),
            "std": round(float(anomaly_scores.std()), 4),
            "min": round(float(anomaly_scores.min()), 4),
            "max": round(float(anomaly_scores.max()), 4),
        },
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
