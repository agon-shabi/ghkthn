from datetime import datetime, time

from dagster import repository, sensor, RunRequest, daily_schedule

from breakfast import breakfast_pipeline
from hello_cereal import hello_pipeline
from input import inputs_pipeline
from complex_pipeline import complex_pipeline
from multiple_inputs_outputs import multiple_input_outputs_pipeline


@daily_schedule(
    pipeline_name="multiple_input_outputs_pipeline",
    start_date=datetime(2021, 1, 1),
    execution_time=time(7, 30),
    execution_timezone="Europe/London",  # "UTC"
)
def good_morning_schedule(date):
    return {
        "solids": {"load_cereals": {"inputs": {"csv_path": {"value": "cereal.csv"}}}}
    }


@sensor(pipeline_name="breakfast_pipeline")
def healthy_cereal_sensor(_context):
    """
        How to find the materialized Asset(
            asset_key="cereal_choice",
            description="Which cereal is healthiest",
            metadata_entries=[EventMetadataEntry.text("Cereal Choice", "Cereal Choice")],
        )
    """
    #   all_keys = get_all_keys(gcs)
    all_keys = ["cereal_choice"]
    for cereal_key in all_keys:
        yield RunRequest(run_key=cereal_key)


@repository
def hello_cereal_repository():
    # Note that we can pass a dict of functions, rather than a list of
    # pipeline definitions. This allows us to construct pipelines lazily,
    # if, e.g., initializing a pipeline involves any heavy compute

    # return {
    #     "pipelines": {
    #         "hello_pipeline": lambda: hello_pipeline,
    #         "inputs_pipeline": lambda: inputs_pipeline,
    #         "complex_pipeline": lambda: complex_pipeline,
    #         "multiple_input_outputs_pipeline": lambda: multiple_input_outputs_pipeline,
    #         "breakfast_pipeline": lambda: breakfast_pipeline,
    #     },
    #     "schedules": {"good_morning_schedule": lambda: good_morning_schedule}
    #
    # } # how to add sensors here?
    return [
        hello_pipeline,
        inputs_pipeline,
        complex_pipeline,
        multiple_input_outputs_pipeline,
        breakfast_pipeline,
        good_morning_schedule,
        healthy_cereal_sensor
    ]
