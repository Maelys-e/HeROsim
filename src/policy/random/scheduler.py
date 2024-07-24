from __future__ import annotations

import random

from typing import Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Task

from src.placement.model import SystemState

from src.placement.scheduler import BaseScheduler


class RandomScheduler(BaseScheduler):
    def placement(self, system_state: SystemState, task: Task):
        # Scheduling functions called in a Simpy Process must be Generators
        # No-op as per https://stackoverflow.com/a/68628599/9568489
        if False:
            yield

        replicas: Set[Tuple[Node, Platform]] = system_state.replicas[task.type["name"]]

        # Select random platform
        found = random.randint(0, len(replicas) - 1)
        random_couple = list(replicas)[found]

        return random_couple
