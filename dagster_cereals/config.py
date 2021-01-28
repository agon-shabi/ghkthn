from dagster import execute_pipeline, pipeline, solid
from dagster import Field, Bool
from input import load_cereals

"""To config the solid parameters by given the config_schema argument"""


# @solid(config_schema={"reverse": bool})
@solid(
    config_schema={
        "reverse": Field(
            Bool,
            default_value=False,
            is_required=False,
            description="If `True`, cereals will be sorted in reverse order. Default: `False`",
        )
    }
)
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(
        cereals,
        key=lambda cereal: int(cereal["calories"]),
        reverse=context.solid_config["reverse"],
    )

    if context.solid_config["reverse"]:  # find the most caloric cereal
        context.log.info(
            "{x} caloric cereal: {first_cereal_after_sort}".format(
                x="Most", first_cereal_after_sort=sorted_cereals[0]["name"]
            )
        )
        return {
            "most_caloric": sorted_cereals[0],
            "least_caloric": sorted_cereals[-1],
        }
    else:  # find the least caloric cereal
        context.log.info(
            "{x} caloric cereal: {first_cereal_after_sort}".format(
                x="Least", first_cereal_after_sort=sorted_cereals[0]["name"]
            )
        )
        return {
            "least_caloric": sorted_cereals[0],
            "most_caloric": sorted_cereals[-1],
        }


@pipeline
def inputs_pipeline():
    sort_by_calories(load_cereals())


def main():
    """Specifying Config for Pipeline Execution"""

    run_config = {
        "solids": {
            "load_cereals": {"inputs": {"csv_path": {"value": "cereal.csv"}}},
            "sort_by_calories": {"config": {"reverse": True}},
        }
    }

    result = execute_pipeline(inputs_pipeline, run_config=run_config)


if __name__ == "__main__":
    main()
