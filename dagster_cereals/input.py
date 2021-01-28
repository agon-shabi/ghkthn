import csv
import os

from dagster import execute_pipeline, pipeline, solid


@solid
def load_cereals(context, csv_path):
    """Parametrizing Solids with Inputs"""

    csv_path = os.path.join(os.path.dirname(__file__), csv_path)
    with open(csv_path, "r") as fd:
        cereals = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} cereals".format(n_lines=len(cereals)))
    return cereals


@solid
def sort_by_cal(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: int(cereal["calories"]))
    context.log.info(
        "Least caloric cereal: {least_caloric}".format(
            least_caloric=sorted_cereals[0]["name"]
        )
    )
    context.log.info(
        "Most caloric cereal: {most_caloric}".format(
            most_caloric=sorted_cereals[-1]["name"]
        )
    )
    return {
        "least_caloric": sorted_cereals[0],
        "most_caloric": sorted_cereals[-1],
    }


@pipeline
def inputs_pipeline():
    sort_by_cal(load_cereals())


def main():
    """Specifying Config for Pipeline Execution"""

    run_config = {
        "solids": {"load_cereals": {"inputs": {"csv_path": {"value": "cereal.csv"}}}}
    }

    result = execute_pipeline(inputs_pipeline, run_config=run_config)


if __name__ == "__main__":
    main()
