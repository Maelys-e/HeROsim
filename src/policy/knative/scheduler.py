from __future__ import annotations

from typing import Generator, Set, Tuple, TYPE_CHECKING


if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Task

from src.placement.model import SystemState

from src.placement.scheduler import BaseScheduler


class KnativeScheduler(BaseScheduler):
    def placement(self, system_state: SystemState, task: Task) -> Generator:
        # Scheduling functions called in a Simpy Process must be Generators
        # No-op as per https://stackoverflow.com/a/68628599/9568489
        if False:
            yield

        replicas: Set[Tuple[Node, Platform]] = system_state.replicas[task.type["name"]]

        # Least Connected
        bounded_concurrency = min(
            replicas, key=lambda couple: len(couple[1].queue.items)
        )

        return bounded_concurrency
