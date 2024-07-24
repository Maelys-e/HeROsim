from __future__ import annotations

from typing import Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Task

from src.placement.model import SchedulerState, SystemState

from src.placement.scheduler import BaseScheduler


class RoundRobinScheduler(BaseScheduler):
    def placement(self, system_state: SystemState, task: Task):
        # Scheduling functions called in a Simpy Process must be Generators
        # No-op as per https://stackoverflow.com/a/68628599/9568489
        if False:
            yield

        replicas: Set[Tuple[Node, Platform]] = system_state.replicas[task.type["name"]]
        state: SchedulerState = system_state.scheduler_state

        # Find least scheduled replica
        least_scheduled = min(
            replicas,
            key=lambda couple: state.scheduled_count[task.type["name"]][
                (couple[0].id, couple[1].id)
            ],
        )

        # Update scheduler state
        state.scheduled_count[task.type["name"]][
            (least_scheduled[0].id, least_scheduled[1].id)
        ] += 1

        return least_scheduled
