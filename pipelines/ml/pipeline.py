"""Vertex AI Pipeline: K-Means Clustering for Player Role Identification."""

from kfp import dsl, compiler

from components.validate_data import validate_data
from components.preprocess import preprocess
from components.train import train
from components.evaluate import evaluate
from components.deploy import deploy_model
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.player_features_v"
BQ_DATASET = "curated"
GCS_BUCKET = "gs://data-feeder-lcd-ml-artifacts"
PIPELINE_NAME = "player-role-clustering"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="K-Means clustering to identify player roles and hidden impact players",
)
def player_clustering_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
    gcs_bucket: str = GCS_BUCKET,
    min_k: int = 3,
    max_k: int = 10,
):
    # Step 0: Validate data quality
    validate_task = validate_data(
        project_id=project_id,
        bq_view=bq_view,
    )

    # Step 1: Preprocess
    preprocess_task = preprocess(
        project_id=project_id,
        bq_view=bq_view,
    )
    preprocess_task.after(validate_task)

    # Step 2: Train
    train_task = train(
        dataset=preprocess_task.outputs["output_dataset"],
        feature_columns=preprocess_task.outputs["feature_columns"],
        min_k=min_k,
        max_k=max_k,
    )

    # Step 3: Evaluate + write clusters to BigQuery
    evaluate_task = evaluate(
        dataset=preprocess_task.outputs["output_dataset"],
        model=train_task.outputs["model"],
        metrics=train_task.outputs["metrics"],
        feature_columns=preprocess_task.outputs["feature_columns"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )

    # Step 4: Deploy model to Vertex AI endpoint
    deploy_task = deploy_model(
        model=train_task.outputs["model"],
        metrics=train_task.outputs["metrics"],
        project_id=project_id,
        region=region,
        gcs_bucket=gcs_bucket,
    )
    deploy_task.after(evaluate_task)

    # Step 5: Log metrics to BigQuery
    log_metrics(
        metrics_artifact=train_task.outputs["metrics"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name="player-role-clustering",
    ).after(evaluate_task)


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=player_clustering_pipeline,
        package_path="pipeline.json",
    )
    print("Pipeline compiled to pipeline.json")
