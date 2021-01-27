from dagster import repository

from runner.dags import Dags


@repository
def tyche_dag_repo():
    return [
        Dags.TEST_PIPELINE,
    ]
