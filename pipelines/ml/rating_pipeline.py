"""Vertex AI Pipeline: Rating Prediction for Players."""

from kfp import dsl, compiler

from components.validate_data import validate_data
from components.predict_ratings import predict_ratings
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.player_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "player-rating-prediction"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="GradientBoosting regression to predict player ratings and find over/under-rated players",
)
def player_rating_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    # Step 0: Validate data quality
    validate_task = validate_data(
        project_id=project_id,
        bq_view=bq_view,
    )

    # Step 1: Read raw data, train, write predictions
    # Does NOT use preprocess since it needs un-normalized rating as target
    predict_task = predict_ratings(
        project_id=project_id,
        region=region,
        bq_view=bq_view,
        bq_dataset=bq_dataset,
    )
    predict_task.after(validate_task)

    # Step 2: Log metrics to BigQuery
    log_metrics(
        metrics_artifact=predict_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name="player-rating-prediction",
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=player_rating_pipeline,
        package_path="rating_pipeline.json",
    )
    print("Rating pipeline compiled to rating_pipeline.json")
