import pandas as pd

from dagster import solid, pipeline, execute_pipeline
from dagster import Field, Bool, OutputDefinition, Output

from input import load_cereals

import dagster_aws

@solid
def prepare_breakfast(context):
    context.log.info(f"Prepare healthy breakfast. (Run_id: {context.run_id})")

    yield Output("Breakfast Ready")


@pipeline
def breakfast_pipeline():
    prepare_breakfast()


def main():
    """Specifying Config for Pipeline Execution"""

    run_config = {
        "solids": {
            "load_cereals": {"inputs": {"csv_path": {"value": "cereal.csv"}}},
            "split_cereals": {"config": {"process_hot": True, "process_cold": True}},
        }
    }

    result = execute_pipeline(breakfast_pipeline)


if __name__ == "__main__":
    main()
