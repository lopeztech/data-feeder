"""Analyze F1 pit stop strategy impact on race positions using GradientBoosting."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def f1_pitstop_analyzer(
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Read pitstop features, aggregate per race-driver, predict positions gained.

    Trains GradientBoosting on pit stop strategy features (stop count, duration,
    timing) to quantify their impact on race positions. Also computes per-constructor
    pit crew performance stats.
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
        f"SELECT * FROM `{project_id}.{bq_dataset}.f1_pitstop_features_v`"
    ).to_dataframe()

    print(f"Loaded {len(df)} pit stop rows")

    # Aggregate per race-driver
    race_driver = df.groupby(["raceId", "driverId"]).agg(
        year=("year", "first"),
        race_name=("race_name", "first"),
        driver=("driver", "first"),
        constructor=("constructor", "first"),
        grid=("grid", "first"),
        finish_position=("finish_position", "first"),
        positions_gained=("positions_gained", "first"),
        total_laps=("total_laps", "first"),
        total_stops=("stop_number", "max"),
        avg_stop_duration=("stop_duration_sec", "mean"),
        min_stop_duration=("stop_duration_sec", "min"),
        total_pit_time_ms=("stop_duration_ms", "sum"),
        first_stop_lap=("stop_lap", "min"),
        last_stop_lap=("stop_lap", "max"),
    ).reset_index()

    # First stop as percentage of race
    race_driver["first_stop_pct"] = np.round(
        race_driver["first_stop_lap"] / race_driver["total_laps"].replace(0, np.nan), 3
    )
    race_driver["avg_stop_duration"] = race_driver["avg_stop_duration"].round(3)
    race_driver["min_stop_duration"] = race_driver["min_stop_duration"].round(3)

    print(f"Aggregated to {len(race_driver)} race-driver entries")

    # Features and target
    target_col = "positions_gained"
    feature_cols = ["total_stops", "avg_stop_duration", "min_stop_duration",
                    "first_stop_pct", "grid", "year"]

    X_raw = race_driver[feature_cols].fillna(0).astype(float)
    scaler = StandardScaler()
    X = scaler.fit_transform(X_raw)
    y = race_driver[target_col].fillna(0).astype(float).values

    print(f"Training on {len(race_driver)} records, {len(feature_cols)} features, "
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

    residuals = np.round(cv_predictions - y, 4)
    predicted = np.round(cv_predictions, 4)

    r2 = round(float(cv_scores.mean()), 4)
    mae = round(float(mae_scores.mean()), 4)
    rmse = round(float(np.sqrt(np.mean(residuals ** 2))), 4)

    print(f"R2={r2}, MAE={mae}, RMSE={rmse}")
    print(f"Top features: {list(sorted_importances.keys())[:5]}")

    # Write pit stop analysis to BigQuery
    bq_client = bigquery.Client(project=project_id, location=region)

    analysis_df = pd.DataFrame({
        "raceId": race_driver["raceId"].astype(int),
        "year": race_driver["year"].astype(int),
        "race_name": race_driver["race_name"].astype(str),
        "driver": race_driver["driver"].astype(str),
        "constructor": race_driver["constructor"].astype(str),
        "grid": race_driver["grid"].astype(int),
        "finish_position": race_driver["finish_position"].astype(int),
        "positions_gained": race_driver["positions_gained"].astype(int),
        "total_stops": race_driver["total_stops"].astype(int),
        "avg_stop_duration": race_driver["avg_stop_duration"],
        "total_pit_time_ms": race_driver["total_pit_time_ms"].astype(int),
        "predicted_positions_gained": predicted,
        "residual": residuals,
    })

    analysis_table = f"{project_id}.{bq_dataset}.f1_pitstop_analysis"
    analysis_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("raceId", "INT64"),
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("race_name", "STRING"),
            bigquery.SchemaField("driver", "STRING"),
            bigquery.SchemaField("constructor", "STRING"),
            bigquery.SchemaField("grid", "INT64"),
            bigquery.SchemaField("finish_position", "INT64"),
            bigquery.SchemaField("positions_gained", "INT64"),
            bigquery.SchemaField("total_stops", "INT64"),
            bigquery.SchemaField("avg_stop_duration", "FLOAT64"),
            bigquery.SchemaField("total_pit_time_ms", "INT64"),
            bigquery.SchemaField("predicted_positions_gained", "FLOAT64"),
            bigquery.SchemaField("residual", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(analysis_df, analysis_table, job_config=analysis_config).result()
    print(f"Wrote {len(analysis_df)} rows to {analysis_table}")

    # Write feature importances to BigQuery
    imp_rows = []
    for rank, (feat, imp) in enumerate(sorted_importances.items(), start=1):
        imp_rows.append({"feature_name": feat, "importance": imp, "rank": rank})
    importances_df = pd.DataFrame(imp_rows)

    imp_table = f"{project_id}.{bq_dataset}.f1_pitstop_feature_importances"
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

    # Compute per-constructor pit stats
    constructor_stats = df.groupby(["constructor", "year"]).agg(
        avg_pit_duration=("stop_duration_sec", "mean"),
        min_pit_duration=("stop_duration_sec", "min"),
        total_pit_stops=("stop_number", "count"),
        avg_positions_gained=("positions_gained", "mean"),
    ).reset_index()
    constructor_stats["avg_pit_duration"] = constructor_stats["avg_pit_duration"].round(3)
    constructor_stats["min_pit_duration"] = constructor_stats["min_pit_duration"].round(3)
    constructor_stats["avg_positions_gained"] = constructor_stats["avg_positions_gained"].round(3)
    constructor_stats["total_pit_stops"] = constructor_stats["total_pit_stops"].astype(int)

    pit_stats_table = f"{project_id}.{bq_dataset}.f1_constructor_pit_stats"
    pit_stats_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("constructor", "STRING"),
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("avg_pit_duration", "FLOAT64"),
            bigquery.SchemaField("min_pit_duration", "FLOAT64"),
            bigquery.SchemaField("total_pit_stops", "INT64"),
            bigquery.SchemaField("avg_positions_gained", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(constructor_stats, pit_stats_table, job_config=pit_stats_config).result()
    print(f"Wrote {len(constructor_stats)} rows to {pit_stats_table}")

    # Fastest pit crews (lowest avg duration, recent years)
    recent = constructor_stats[constructor_stats["year"] >= 2015].copy()
    fastest_crews = recent.sort_values("avg_pit_duration").head(10)[
        ["constructor", "year", "avg_pit_duration", "total_pit_stops"]
    ].to_dict("records")

    # Report
    report_data = {
        "total_race_driver_entries": len(race_driver),
        "total_pit_stops": len(df),
        "r2": r2,
        "mae": mae,
        "rmse": rmse,
        "cv_r2_std": round(float(cv_scores.std()), 4),
        "feature_importances": sorted_importances,
        "fastest_pit_crews": fastest_crews,
        "residual_stats": {
            "mean": round(float(residuals.mean()), 4),
            "std": round(float(residuals.std()), 4),
            "min": round(float(residuals.min()), 4),
            "max": round(float(residuals.max()), 4),
        },
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
