"""Validate data: run quality checks on the feature view before training."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-bigquery==3.30.0", "pandas==2.2.3", "db-dtypes==1.3.1",
        "pyarrow==18.1.0", "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def validate_data(
    project_id: str,
    bq_view: str,
    min_rows: int = 50,
    max_null_pct: float = 0.5,
) -> int:
    """Run data quality checks on the feature view. Fails the pipeline if checks don't pass.

    Checks:
    1. Minimum row count
    2. No columns are entirely NULL
    3. No column exceeds max_null_pct NULL rate
    4. Rating is within expected range (0-10)
    5. No duplicate player_ids
    6. Numeric features have non-zero variance (not all identical)

    Returns the row count on success.
    """
    import pandas as pd
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    df = client.query(f"SELECT * FROM `{bq_view}`").to_dataframe()

    errors: list[str] = []

    # 1. Minimum row count
    if len(df) < min_rows:
        errors.append(f"Row count {len(df)} is below minimum threshold {min_rows}")

    # 2-3. NULL checks per column
    for col in df.columns:
        null_pct = df[col].isna().mean()
        if null_pct == 1.0:
            errors.append(f"Column '{col}' is 100% NULL")
        elif null_pct > max_null_pct:
            errors.append(f"Column '{col}' is {null_pct:.1%} NULL (threshold: {max_null_pct:.0%})")

    # 4. Rating range
    if "rating" in df.columns:
        rating = pd.to_numeric(df["rating"], errors="coerce")
        out_of_range = ((rating < 0) | (rating > 10)).sum()
        if out_of_range > 0:
            errors.append(f"{out_of_range} ratings outside 0-10 range")

    # 5. Duplicate player_ids
    if "player_id" in df.columns:
        dupes = df["player_id"].duplicated().sum()
        if dupes > 0:
            errors.append(f"{dupes} duplicate player_id values")

    # 6. Zero-variance numeric columns
    drop_cols = {"player_id", "position", "league"}
    numeric_cols = [c for c in df.columns if c not in drop_cols and pd.api.types.is_numeric_dtype(df[c])]
    for col in numeric_cols:
        if df[col].std() == 0:
            errors.append(f"Column '{col}' has zero variance (all values identical)")

    if errors:
        msg = "Data validation FAILED:\n" + "\n".join(f"  - {e}" for e in errors)
        print(msg)
        raise ValueError(msg)

    print(f"Data validation PASSED: {len(df)} rows, {len(df.columns)} columns, all checks OK")
    return len(df)
