"""Train win predictor: GradientBoosting regression on win_loss_perc, write predictions to BigQuery."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def train_win_predictor(
    project_id: str,
    region: str,
    bq_view: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Read team features, train two GradientBoosting models on win_loss_perc, write predictions to BQ.

    Two models are trained:
    - all_features: uses all available features (minus identifiers and target-related)
    - controllable: additionally excludes tautological features (points_diff, mov, etc.)

    Uses TimeSeriesSplit for cross-validation to respect temporal structure.
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
    df = client.query(f"SELECT * FROM `{bq_view}`").to_dataframe()

    # Sort by year so TimeSeriesSplit works correctly
    df = df.sort_values("year").reset_index(drop=True)

    target_col = "win_loss_perc"
    id_cols = ["year", "team"]
    target_related = ["wins", "losses", "ties", "is_playoff", "win_loss_perc"]
    tautological = ["points_diff", "mov", "exp_pts_tot", "score_pct", "turnover_pct"]

    all_drop = id_cols + target_related
    controllable_drop = id_cols + target_related + tautological

    all_feature_cols = [c for c in df.columns if c not in all_drop]
    controllable_feature_cols = [c for c in df.columns if c not in controllable_drop]

    y = df[target_col].fillna(0).astype(float).values
    kfold = KFold(n_splits=5, shuffle=True, random_state=42)

    gbr_params = dict(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        min_samples_leaf=10,
        random_state=42,
    )

    all_predictions = []
    all_importances = []
    report_data = {}

    for model_type, feature_cols in [("all_features", all_feature_cols), ("controllable", controllable_feature_cols)]:
        X_raw = df[feature_cols].fillna(0).astype(float)
        scaler = StandardScaler()
        X = scaler.fit_transform(X_raw)

        print(f"\n[{model_type}] Training on {len(df)} records, {len(feature_cols)} features")
        print(f"  Target range: {y.min():.3f} - {y.max():.3f}")

        model = GradientBoostingRegressor(**gbr_params)

        # TimeSeriesSplit CV predictions
        cv_predictions = cross_val_predict(model, X, y, cv=kfold)

        # CV scores for metrics
        cv_scores = cross_val_score(model, X, y, cv=kfold, scoring="r2")
        mae_scores = -cross_val_score(model, X, y, cv=kfold, scoring="neg_mean_absolute_error")

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

        print(f"  R2={r2}, MAE={mae}, RMSE={rmse}")
        print(f"  Top features: {list(sorted_importances.keys())[:5]}")

        # Collect predictions
        pred_df = pd.DataFrame({
            "year": df["year"].values,
            "team": df["team"].astype(str).values,
            "actual_win_pct": actual,
            "predicted_win_pct": predicted,
            "residual": residuals,
            "model_type": model_type,
        })
        all_predictions.append(pred_df)

        # Collect feature importances
        for rank, (feat, imp) in enumerate(sorted_importances.items(), start=1):
            all_importances.append({
                "feature_name": feat,
                "importance": imp,
                "rank": rank,
                "model_type": model_type,
            })

        report_data[model_type] = {
            "r2": r2,
            "mae": mae,
            "rmse": rmse,
            "cv_r2_std": round(float(cv_scores.std()), 4),
            "n_features": len(feature_cols),
            "feature_importances": sorted_importances,
            "residual_stats": {
                "mean": round(float(residuals.mean()), 4),
                "std": round(float(residuals.std()), 4),
                "min": round(float(residuals.min()), 4),
                "max": round(float(residuals.max()), 4),
            },
        }

    # Write predictions to BigQuery
    predictions_df = pd.concat(all_predictions, ignore_index=True)
    bq_client = bigquery.Client(project=project_id, location=region)

    pred_table = f"{project_id}.{bq_dataset}.team_win_predictions"
    pred_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("team", "STRING"),
            bigquery.SchemaField("actual_win_pct", "FLOAT64"),
            bigquery.SchemaField("predicted_win_pct", "FLOAT64"),
            bigquery.SchemaField("residual", "FLOAT64"),
            bigquery.SchemaField("model_type", "STRING"),
        ],
    )
    bq_client.load_table_from_dataframe(predictions_df, pred_table, job_config=pred_config).result()
    print(f"\nWrote {len(predictions_df)} rows to {pred_table}")

    # Write feature importances to BigQuery
    importances_df = pd.DataFrame(all_importances)
    imp_table = f"{project_id}.{bq_dataset}.team_feature_importances"
    imp_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("feature_name", "STRING"),
            bigquery.SchemaField("importance", "FLOAT64"),
            bigquery.SchemaField("rank", "INT64"),
            bigquery.SchemaField("model_type", "STRING"),
        ],
    )
    bq_client.load_table_from_dataframe(importances_df, imp_table, job_config=imp_config).result()
    print(f"Wrote {len(importances_df)} rows to {imp_table}")

    # Report
    report_data["total_teams"] = len(df)
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
