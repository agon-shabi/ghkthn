from typing import Dict

from dagster import (
    OutputDefinition,
    solid,
    Output,
)

from dagster_gcp.gcs.resources import gcs_resource
from dagster_gcp.gcs.system_storage import gcs_plus_default_intermediate_storage_defs
from dagster import ModeDefinition


@solid(
    name="SolidOne",
    description=r"""
    SolidOne
    """,
    input_defs=[],
    output_defs=[
        OutputDefinition(name="cfg", dagster_type=Dict),
        OutputDefinition(name="args", dagster_type=Dict),
    ],
    config_schema={"args": dict},
)
def stage_one(context):

    prod_mode = ModeDefinition(
        name='prod',
        intermediate_storage_defs=gcs_plus_default_intermediate_storage_defs,
        resource_defs={'gcs': gcs_resource}
    )

    yield Output(cfg, output_name="cfg")
    yield Output(context.solid_config["args"], output_name="args")
