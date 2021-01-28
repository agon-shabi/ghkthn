import csv
import os

from dagster import execute_pipeline, pipeline, solid


@solid
def hello_cereal(context):
    """
    A solid is a unit of computation in a data pipeline. Typically, one will
    define solids by annotating ordinary Python functions with the @solid
    decorator. Note that the context is provided by the Dagster framework
    as the first argument to every solid.
    """
    # Assuming the dataset is in the same directory as this file
    dataset_path = os.path.join(os.path.dirname(__file__), "cereal.csv")
    with open(dataset_path, "r") as fd:
        # Read the rows in using the standard csv library
        cereals = [row for row in csv.DictReader(fd)]

    context.log.info(
        "Found {n_cereals} cereals".format(n_cereals=len(cereals))
    )

    return cereals

@pipeline
def hello_pipeline():
    """
    This call doesn't actually execute the solid—within the body of functions
    decorated with @pipeline. Dagster uses function calls to indicate the
    dependency structure of the solids making up the pipeline.
    Here, it is indicated that the execution of hello_cereal doesn't depend on
    any other solids by calling it with no arguments.
"""
    hello_cereal()
