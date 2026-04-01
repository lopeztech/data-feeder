"""Vertex AI Pipeline: Rating Prediction for Players."""

from kfp import dsl, compiler

from components.preprocess import preprocess
from components.predict_ratings import predict_ratings

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
    # Step 1: Preprocess (reuse from clustering pipeline)
    preprocess_task = preprocess(
        project_id=project_id,
        bq_view=bq_view,
    )

    # Step 2: Train + predict + write to BigQuery
    predict_ratings(
        dataset=preprocess_task.outputs["output_dataset"],
        feature_columns=preprocess_task.outputs["feature_columns"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=player_rating_pipeline,
        package_path="rating_pipeline.json",
    )
    print("Rating pipeline compiled to rating_pipeline.json")
