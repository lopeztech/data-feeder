"""Vertex AI Pipeline: Rating Prediction for Players."""

from kfp import dsl, compiler

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
    # Single step: reads raw data from BQ, trains, writes predictions
    # Does NOT use preprocess since it needs un-normalized rating as target
    predict_ratings(
        project_id=project_id,
        region=region,
        bq_view=bq_view,
        bq_dataset=bq_dataset,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=player_rating_pipeline,
        package_path="rating_pipeline.json",
    )
    print("Rating pipeline compiled to rating_pipeline.json")
