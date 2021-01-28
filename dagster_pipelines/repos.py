from gcs_example.gcs_example import example_pipe
from iris_example.batch_dag import full_run

from dagster import repository


@repository
def repo():
    return [example_pipe, full_run]
