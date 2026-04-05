"""Compute positional weights: bridge team feature importances to positional value."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "pandas==2.2.3", "scikit-learn==1.6.1",
        "pyarrow==18.1.0", "google-cloud-bigquery==3.30.0",
        "db-dtypes==1.3.1", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def compute_positional_weights(
    project_id: str,
    region: str,
    bq_dataset: str,
    report: dsl.Output[dsl.Artifact],
):
    """Read team feature importances, apply position-contribution matrix, write positional values.

    Uses hardcoded position-contribution weights to distribute team-level feature
    importance across football positions (QB, RB, WR, TE, OL, DEF, etc.).
    """
    import json
    import pandas as pd
    from google.cloud import bigquery

    POSITION_CONTRIBUTIONS = {
        "pass_yds_per_game": {"QB": 0.55, "WR": 0.25, "TE": 0.10, "OL": 0.10},
        "pass_td_pct": {"QB": 0.55, "WR": 0.25, "TE": 0.10, "OL": 0.10},
        "pass_completion_pct": {"QB": 0.60, "WR": 0.20, "TE": 0.10, "OL": 0.10},
        "pass_net_yds_per_att": {"QB": 0.50, "WR": 0.25, "TE": 0.10, "OL": 0.15},
        "interception_pct": {"QB": 0.70, "WR": 0.10, "TE": 0.05, "OL": 0.15},
        "rush_yds_per_game": {"RB": 0.55, "QB": 0.10, "OL": 0.35},
        "rush_yds_per_att": {"RB": 0.50, "QB": 0.10, "OL": 0.40},
        "rush_td": {"RB": 0.60, "QB": 0.15, "OL": 0.25},
        "yards_per_game": {"QB": 0.30, "WR": 0.20, "RB": 0.20, "TE": 0.10, "OL": 0.20},
        "first_down": {"QB": 0.30, "WR": 0.20, "RB": 0.20, "TE": 0.10, "OL": 0.20},
        "turnovers": {"QB": 0.50, "RB": 0.20, "WR": 0.15, "TE": 0.10, "DEF": 0.05},
        "fumbles_lost": {"RB": 0.40, "QB": 0.25, "WR": 0.20, "TE": 0.10, "DEF": 0.05},
        "penalties": {"OL": 0.35, "DEF": 0.30, "WR": 0.15, "DB": 0.10, "LB": 0.10},
        "penalties_yds": {"OL": 0.35, "DEF": 0.30, "WR": 0.15, "DB": 0.10, "LB": 0.10},
    }

    # All known positions from the contribution matrix
    ALL_POSITIONS = sorted({pos for mapping in POSITION_CONTRIBUTIONS.values() for pos in mapping})

    # Read feature importances (controllable model only)
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT feature_name, importance
        FROM `{project_id}.{bq_dataset}.team_feature_importances`
        WHERE model_type = 'controllable'
        ORDER BY importance DESC
    """
    imp_df = client.query(query).to_dataframe()
    print(f"Read {len(imp_df)} controllable feature importances")

    # Build positional value rows
    rows = []
    for _, row in imp_df.iterrows():
        feature = row["feature_name"]
        importance = float(row["importance"])

        if feature in POSITION_CONTRIBUTIONS:
            contributions = POSITION_CONTRIBUTIONS[feature]
        else:
            # Default: split evenly across all positions
            even_weight = round(1.0 / len(ALL_POSITIONS), 4)
            contributions = {pos: even_weight for pos in ALL_POSITIONS}

        for position, weight in contributions.items():
            rows.append({
                "position": position,
                "team_feature": feature,
                "contribution_weight": round(weight, 4),
                "team_feature_importance": round(importance, 4),
                "positional_value": round(weight * importance, 6),
            })

    output_df = pd.DataFrame(rows)

    # Write to BigQuery
    bq_client = bigquery.Client(project=project_id, location=region)
    table_id = f"{project_id}.{bq_dataset}.positional_value_weights"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("position", "STRING"),
            bigquery.SchemaField("team_feature", "STRING"),
            bigquery.SchemaField("contribution_weight", "FLOAT64"),
            bigquery.SchemaField("team_feature_importance", "FLOAT64"),
            bigquery.SchemaField("positional_value", "FLOAT64"),
        ],
    )
    bq_client.load_table_from_dataframe(output_df, table_id, job_config=job_config).result()
    print(f"Wrote {len(output_df)} rows to {table_id}")

    # Aggregate by position for report
    pos_totals = output_df.groupby("position")["positional_value"].sum().sort_values(ascending=False)
    top_positions = [{"position": pos, "total_value": round(float(val), 4)} for pos, val in pos_totals.items()]

    # Top features per position
    top_per_position = {}
    for pos in pos_totals.index:
        pos_rows = output_df[output_df["position"] == pos].sort_values("positional_value", ascending=False)
        top_per_position[pos] = [
            {"feature": r["team_feature"], "value": round(r["positional_value"], 4)}
            for _, r in pos_rows.head(3).iterrows()
        ]

    # Report
    report_data = {
        "total_rows": len(output_df),
        "n_features": len(imp_df),
        "n_positions": len(pos_totals),
        "top_positions_by_value": top_positions,
        "top_features_per_position": top_per_position,
    }
    with open(report.path, "w") as f:
        json.dump(report_data, f, indent=2)
