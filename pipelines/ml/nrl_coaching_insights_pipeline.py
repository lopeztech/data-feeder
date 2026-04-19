"""Vertex AI Pipeline: NRL Coaching Insights Analyzer."""

from kfp import dsl, compiler

from components.team_validate_data import validate_team_data
from components.nrl_coaching_analyzer import nrl_coaching_analyzer
from components.log_metrics import log_metrics

PROJECT_ID = "data-feeder-lcd"
REGION = "australia-southeast1"
BQ_VIEW = f"{PROJECT_ID}.curated.nrl_coaching_features_v"
BQ_DATASET = "curated"
PIPELINE_NAME = "nrl-coaching-insights"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="Tactical clustering, SWOT profiles, rivalry matrix, and trend analysis for NRL coaches",
)
def nrl_coaching_insights_pipeline(
    project_id: str = PROJECT_ID,
    region: str = REGION,
    bq_view: str = BQ_VIEW,
    bq_dataset: str = BQ_DATASET,
):
    validate_task = validate_team_data(
        project_id=project_id,
        bq_view=bq_view,
        min_rows=50,
        max_wins=30,
        max_losses=30,
    )

    coaching_task = nrl_coaching_analyzer(
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
    )
    coaching_task.after(validate_task)

    log_metrics(
        metrics_artifact=coaching_task.outputs["report"],
        project_id=project_id,
        region=region,
        bq_dataset=bq_dataset,
        pipeline_name=PIPELINE_NAME,
    )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=nrl_coaching_insights_pipeline,
        package_path="nrl_coaching_insights_pipeline.json",
    )
    print("Pipeline compiled to nrl_coaching_insights_pipeline.json")
