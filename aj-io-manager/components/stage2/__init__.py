from typing import Dict

from dagster import (
    OutputDefinition,
    InputDefinition,
    solid,
    Output,

)
from dagster_pandas import DataFrame
import pandas as pd



@solid(
    name="SolidTwo",
    description=r"""
    SolidTwo
    """,
    input_defs=[InputDefinition(name="cfg", dagster_type=Dict)],
    output_defs=[
        OutputDefinition(name="df", dagster_type=DataFrame),
    ],
)
def stage_two(context,cfg):

    context.log.info(f"read cfg from last solid: {cfg}")
    df = pd.DataFrame(columns=["a", "b"], data=[[1, 2], [3, 4]])
    return Output(df, output_name="df")
