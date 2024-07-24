from pprint import pprint

from typing import Dict, Set

from src.placement.model import ApplicationType, TaskType, TimeSeries


def get_workload_stats(
    application_types: Dict[str, ApplicationType],
    task_types: Dict[str, TaskType],
    time_series: TimeSeries,
):
    applications_of_tasks: Dict[str, Set[str]] = {}

    for application_type_name in application_types:
        for task_type_name in application_types[application_type_name]["dag"]:
            if task_type_name not in applications_of_tasks:
                applications_of_tasks[task_type_name] = set()

            applications_of_tasks[task_type_name].add(application_type_name)

    pprint(applications_of_tasks)
