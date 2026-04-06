"""Predict F1 race finish positions using GradientBoosting regression."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def f1_driver_predictor(
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Read driver features, train GradientBoosting on finish position, write predictions to BQ.

    Trains on finished races only. Uses grid, quali_position, year, points, laps,
    and finished flag as features. Identifies overperformers via residual analysis.
    """
    import json
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import KFold, cross_val_predict, cross_val_score
    from google.cloud import bigquery

    # Read raw data from the feature view
    client = bigquery.Client(project=project_id)
    df = client.query(
        f"SELECT * FROM `{project_id}.{bq_dataset}.f1_driver_features_v`"
    ).to_dataframe()

    # Filter to finished races only (predict among finishers)
    df = df[df["finished"] == 1].reset_index(drop=True)

    # Fill missing quali_position with grid position
    df["quali_position"] = df["quali_position"].fillna(df["grid"])

    target_col = "finish_position"
    feature_cols = ["grid", "quali_position", "year", "points", "laps", "finished"]
    id_cols = ["resultId", "year", "race_name", "driver", "driver_name", "constructor", "grid"]

    X_raw = df[feature_cols].fillna(0).astype(float)
    scaler = StandardScaler()
    X = scaler.fit_transform(X_raw)
    y = df[target_col].fillna(0).astype(float).values

    print(f"Training on {len(df)} records, {len(feature_cols)} features, "
          f"target range: {y.min():.0f} - {y.max():.0f}")

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        random_state=42,
    )

    # 5-fold CV predictions
    kfold = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_predictions = cross_val_predict(model, X, y, cv=kfold)

    # CV scores for metrics
    cv_scores = cross_val_score(model, X, y, cv=kfold, scoring="r2")
    mae_scores = -cross_val_score(model, X, y, cv=kfold, scoring="neg_mean_absolute_error")

    # Fit final model on all data for feature importances
    model.fit(X, y)
    importances = dict(zip(feature_cols, [round(float(v), 4) for v in model.feature_importances_]))
    sorted_importances = dict(sorted(importances.items(), key=lambda x: x[1], reverse=True))

    # Residuals: positive = finished better than predicted (overperformer)
    residuals = np.round(cv_predictions - y, 4)
    predicted = np.round(cv_predictions, 4)

    r2 = round(float(cv_scores.mean()), 4)
    mae = round(float(mae_scores.mean()), 4)
    rmse = round(float(np.sqrt(np.mean(residuals ** 2))), 4)

    print(f"R2={r2}, MAE={mae}, RMSE={rmse}")
    print(f"Top features: {list(sorted_importances.keys())[:5]}")

    # Write predictions to BigQuery
    output_df = pd.DataFrame({
        "resultId": df["resultId"].astype(int),
        "year": df["year"].astype(int),
        "race_name": df["race_name"].astype(str),
        "driver": df["driver"].astype(str),
        "driver_name": df["driver_name"].astype(str),
        "constructor": df["constructor"].astype(str),
        "grid": df["grid"].astype(int),
        "finish_position": df[target_col].astype(int),
        "predicted_position": predicted,
        "residual": residuals,
    })

    bq_client = bigquery.Client(project=project_id, location=region)

    pred_table = f"{project_id}.{bq_dataset}.f1_driver_predictions"
    pred_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("resultId", "INT64"),
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("race_name", "STRING"),
            bigquery.SchemaField("driver", "STRING"),
            bigquery.SchemaField("driver_name", "STRING"),
            bigquery.SchemaField("constructor", "STRING"),
            bigquery.SchemaField("grid", "INT64"),
            bigquery.SchemaField("finish_position", "INT64"),
            bigquery.SchemaField("predicted_position", "FLOAT64"),
            bigquery.SchemaField("residual", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(output_df, pred_table, job_config=pred_config).result()
    print(f"Wrote {len(output_df)} rows to {pred_table}")

    # Write feature importances to BigQuery
    imp_rows = []
    for rank, (feat, imp) in enumerate(sorted_importances.items(), start=1):
        imp_rows.append({"feature_name": feat, "importance": imp, "rank": rank})
    importances_df = pd.DataFrame(imp_rows)

    imp_table = f"{project_id}.{bq_dataset}.f1_driver_feature_importances"
    imp_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("feature_name", "STRING"),
            bigquery.SchemaField("importance", "FLOAT64"),
            bigquery.SchemaField("rank", "INT64"),
        ],
    )
    bq_client.load_table_from_dataframe(importances_df, imp_table, job_config=imp_config).result()
    print(f"Wrote {len(importances_df)} rows to {imp_table}")

    # Top overperformers (most negative residual = finished much better than predicted)
    output_df_sorted = output_df.sort_values("residual").head(10)
    top_overperformers = output_df_sorted[["driver_name", "race_name", "year", "grid",
                                           "finish_position", "predicted_position", "residual"]].to_dict("records")

    # Report
    report_data = {
        "total_records": len(df),
        "r2": r2,
        "mae": mae,
        "rmse": rmse,
        "cv_r2_std": round(float(cv_scores.std()), 4),
        "feature_importances": sorted_importances,
        "top_overperformers": top_overperformers,
        "residual_stats": {
            "mean": round(float(residuals.mean()), 4),
            "std": round(float(residuals.std()), 4),
            "min": round(float(residuals.min()), 4),
            "max": round(float(residuals.max()), 4),
        },
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
