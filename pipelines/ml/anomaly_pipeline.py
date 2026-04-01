"""Vertex AI Pipeline: Isolation Forest Anomaly Detection for Players."""

from kfp import dsl, compiler

from components.preprocess import preprocess
from components.detect_anomalies import detect_anomalies

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.player_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "player-anomaly-detection"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Isolation Forest anomaly detection to find players with unusual stat distributions",
)
def player_anomaly_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
    contamination: float = 0.05,
):
    # Step 1: Preprocess (reuse from clustering pipeline)
    preprocess_task = preprocess(
        project_id=project_id,
        bq_view=bq_view,
    )

    # Step 2: Detect anomalies + write to BigQuery
    detect_anomalies(
        dataset=preprocess_task.outputs["output_dataset"],
        feature_columns=preprocess_task.outputs["feature_columns"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        contamination=contamination,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=player_anomaly_pipeline,
        package_path="anomaly_pipeline.json",
    )
    print("Anomaly pipeline compiled to anomaly_pipeline.json")
