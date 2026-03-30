"""Vertex AI Pipeline: K-Means Clustering for Player Role Identification."""

from kfp import dsl, compiler

from components.preprocess import preprocess
from components.train import train
from components.evaluate import evaluate
from components.deploy import deploy_model

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.player_features_v"
BQ_DATASET = "curated"
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
    min_k: int = 3,
    max_k: int = 10,
):
    # Step 1: Preprocess
    preprocess_task = preprocess(
        project_id=project_id,
        bq_view=bq_view,
    )

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
    deploy_model(
        model=train_task.outputs["model"],
        metrics=train_task.outputs["metrics"],
        project_id=project_id,
        region=region,
    ).after(evaluate_task)


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=player_clustering_pipeline,
        package_path="pipeline.json",
    )
    print("Pipeline compiled to pipeline.json")
