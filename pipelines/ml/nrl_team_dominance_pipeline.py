"""Vertex AI Pipeline: NRL Team All-Time Dominance Rankings."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.nrl_team_dominance import nrl_team_dominance
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.nrl_team_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "nrl-team-dominance"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Rank NRL teams by composite dominance score and identify archetypes across 35 years",
)
def nrl_team_dominance_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
        min_rows=50,
    )

    dominance_task = nrl_team_dominance(
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )
    dominance_task.after(validate_task)

    log_metrics(
        metrics_artifact=dominance_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=nrl_team_dominance_pipeline,
        package_path="nrl_team_dominance_pipeline.json",
    )
    print("Pipeline compiled to nrl_team_dominance_pipeline.json")
