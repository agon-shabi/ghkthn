from typing import Optional, Callable

from dagster import ModeDefinition
from dagster import pipeline, PresetDefinition
from dagster_gcp.gcs.resources import gcs_resource
from dagster_gcp.gcs.system_storage import gcs_plus_default_intermediate_storage_defs

from components.stage1 import stage_one


preset = PresetDefinition.from_files(
    name="cfg", config_files=["config.yaml"], mode="gcp_mode"
)


class Dags:
    @staticmethod
    @pipeline(
        preset_defs=[preset],
        mode_defs=[
            ModeDefinition(
                name="gcp_mode",
                intermediate_storage_defs=gcs_plus_default_intermediate_storage_defs,
                resource_defs={"gcs": gcs_resource},
            )
        ],
    )
    def TEST_PIPELINE():
        cfg = stage_one()


def get_dag(dag_name: Optional[str] = None) -> Callable:
    return getattr(Dags, dag_name)
