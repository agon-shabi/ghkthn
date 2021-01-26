from typing import Optional, Callable

from dagster import pipeline

from components.stage1 import stage_one

class Dags:
    @staticmethod
    @pipeline
    def TEST_PIPELINE():
        cfg, args = stage_one()



def get_dag(dag_name: Optional[str] = None) -> Callable:
    return getattr(Dags, dag_name)