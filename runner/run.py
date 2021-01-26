import argparse
from typing import Optional, Sequence

from dagster import execute_pipeline

from runner.dags import get_dag

OptionalArgs = Optional[Sequence[str]]


def parse_args(args_list: OptionalArgs = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag", required=True)
    args = parser.parse_args(args_list)
    return args


def main(args_list: OptionalArgs = None):
    args = parse_args(args_list)

    dag = get_dag(args.dag)


    execute_pipeline(pipeline=dag, preset="cfg")


if __name__ == "__main__":
    main()
