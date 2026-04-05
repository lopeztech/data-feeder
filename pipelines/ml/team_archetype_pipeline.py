"""Vertex AI Pipeline: K-Means Clustering for NFL Team Archetypes."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.team_preprocess import team_preprocess
from components.train_team_clusters import train_team_clusters
from components.evaluate_team_clusters import evaluate_team_clusters
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.team_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "team-archetype-clustering"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Identify NFL team playing styles and which archetypes win most",
)
def team_archetype_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
    min_k: int = 3,
    max_k: int = 8,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
    )

    preprocess_task = team_preprocess(
        project_id=project_id,
        bq_view=bq_view,
    )
    preprocess_task.after(validate_task)

    train_task = train_team_clusters(
        dataset=preprocess_task.outputs["output_dataset"],
        feature_columns=preprocess_task.outputs["feature_columns"],
        min_k=min_k,
        max_k=max_k,
    )

    evaluate_task = evaluate_team_clusters(
        dataset=preprocess_task.outputs["output_dataset"],
        raw_dataset=preprocess_task.outputs["raw_dataset"],
        model=train_task.outputs["model"],
        metrics=train_task.outputs["metrics"],
        feature_columns=preprocess_task.outputs["feature_columns"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )

    log_metrics(
        metrics_artifact=train_task.outputs["metrics"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    ).after(evaluate_task)


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=team_archetype_pipeline,
        package_path="team_archetype_pipeline.json",
    )
    print("Pipeline compiled to team_archetype_pipeline.json")
