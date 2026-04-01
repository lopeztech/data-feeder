"""Predict ratings: train GradientBoosting regressor, write predictions to BigQuery."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def predict_ratings(
    dataset: dsl.Input[dsl.Dataset],
    feature_columns: dsl.Input[dsl.Artifact],
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Train GradientBoosting, cross-validate, write player_rating_predictions to BigQuery."""
    import json
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.model_selection import cross_val_predict, cross_val_score
    from google.cloud import bigquery

    df = pd.read_parquet(dataset.path)

    with open(feature_columns.path) as f:
        all_features = json.load(f)

    # Target is 'rating' — remove it from feature set
    target_col = "rating"
    if target_col not in all_features:
        raise ValueError(f"'{target_col}' not in feature columns: {all_features}")

    feature_cols = [c for c in all_features if c != target_col]
    X = df[feature_cols].values
    y = df[target_col].values

    # Train with cross-validation to get unbiased predictions
    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        random_state=42,
    )

    # 5-fold CV predictions (each player predicted by model that didn't see them)
    cv_predictions = cross_val_predict(model, X, y, cv=5)

    # CV scores for metrics
    cv_scores = cross_val_score(model, X, y, cv=5, scoring="r2")
    mae_scores = -cross_val_score(model, X, y, cv=5, scoring="neg_mean_absolute_error")

    # Fit final model on all data for feature importances
    model.fit(X, y)
    importances = dict(zip(feature_cols, [round(float(v), 4) for v in model.feature_importances_]))
    sorted_importances = dict(sorted(importances.items(), key=lambda x: x[1], reverse=True))

    residuals = np.round(cv_predictions - y, 4)
    predicted = np.round(cv_predictions, 4)
    actual = np.round(y, 4)

    r2 = round(float(cv_scores.mean()), 4)
    mae = round(float(mae_scores.mean()), 4)
    rmse = round(float(np.sqrt(np.mean(residuals ** 2))), 4)

    print(f"R2={r2}, MAE={mae}, RMSE={rmse}")
    print(f"Top features: {list(sorted_importances.keys())[:5]}")

    # Write to BigQuery
    output_df = pd.DataFrame({
        "player_id": df["player_id"].astype(str),
        "predicted_rating": predicted,
        "actual_rating": actual,
        "residual": residuals,
    })

    client = bigquery.Client(project=project_id, location=region)
    table_id = f"{project_id}.{bq_dataset}.player_rating_predictions"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("player_id", "STRING"),
            bigquery.SchemaField("predicted_rating", "FLOAT64"),
            bigquery.SchemaField("actual_rating", "FLOAT64"),
            bigquery.SchemaField("residual", "FLOAT64"),
        ],
    )
    client.load_table_from_dataframe(output_df, table_id, job_config=job_config).result()
    print(f"Wrote {len(output_df)} rows to {table_id}")

    # Report
    report_data = {
        "total_players": len(df),
        "r2": r2,
        "mae": mae,
        "rmse": rmse,
        "cv_r2_std": round(float(cv_scores.std()), 4),
        "feature_importances": sorted_importances,
        "residual_stats": {
            "mean": round(float(residuals.mean()), 4),
            "std": round(float(residuals.std()), 4),
            "min": round(float(residuals.min()), 4),
            "max": round(float(residuals.max()), 4),
        },
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
