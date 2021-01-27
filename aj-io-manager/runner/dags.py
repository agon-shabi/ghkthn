from typing import Optional, Callable

from dagster import ModeDefinition
from dagster import pipeline, PresetDefinition
from dagster_gcp.gcs.resources import gcs_resource
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from components.stage1 import stage_one
from components.stage2 import stage_two

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
                resource_defs={'io_manager': gcs_pickle_io_manager, 'gcs': gcs_resource}
            )
        ],
    )
    def TEST_PIPELINE():
        cfg = stage_one()
        df = stage_two(cfg)


def get_dag(dag_name: Optional[str] = None) -> Callable:
    return getattr(Dags, dag_name)
