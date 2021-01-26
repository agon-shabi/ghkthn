from typing import Dict

from dagster import (
    OutputDefinition,
    solid,
    Output,
)



@solid(
    name="SolidOne",
    description=r"""
    SolidOne
    """,
    input_defs=[],
    output_defs=[
        OutputDefinition(name="cfg", dagster_type=Dict),
    ],
)
def stage_one(context):

    context.log.info(f"starting dag runid: {context.run_id}")
    cfg = {"mycol": 1}
    return Output(cfg, output_name="cfg")
