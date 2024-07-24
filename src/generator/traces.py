import random

from typing import List


from src.placement.model import (
    ApplicationType,
    QoSType,
    SimulationData,
    TimeSeries,
    WorkloadEvent,
)


def poisson_process(lambd: int, duration_time: int) -> List[float]:
    arrivals: List[float] = []
    current_time = 0.0

    while current_time < duration_time:
        inter_arrival_time = random.expovariate(lambd)
        current_time += inter_arrival_time

        if current_time < duration_time:
            arrivals.append(current_time)

    return arrivals


def generate_time_series(
    data: SimulationData, rps: int, duration_time: int
) -> TimeSeries:
    # Generate Poisson process arrivals
    arrivals = poisson_process(rps, duration_time)

    events: List[WorkloadEvent] = []

    for timestamp in arrivals:
        application_type_count: int = len(data.application_types)
        application_type_index: int = random.randint(0, application_type_count - 1)
        application_type_name: str = list(data.application_types)[
            application_type_index
        ]
        application_type: ApplicationType = data.application_types[
            application_type_name
        ]

        qos_type_count: int = len(data.qos_types)
        qos_type_index: int = random.randint(0, qos_type_count - 1)
        qos_type_name: str = list(data.qos_types)[qos_type_index]
        qos_type: QoSType = data.qos_types[qos_type_name]

        workload_event: WorkloadEvent = {
            "timestamp": timestamp,
            "application": application_type,
            "qos": qos_type,
        }

        events.append(workload_event)

    time_series = TimeSeries(rps=rps, duration=duration_time, events=events)

    return time_series
