from __future__ import annotations

import random

from typing import Generator, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform

from src.placement.model import SystemState, TaskType

from src.placement.autoscaler import BaseAutoscaler


class RandomAutoscaler(BaseAutoscaler):
    def create_first_replica(
        self, system_state: SystemState, task_type: TaskType
    ) -> Generator:
        stop = yield self.env.process(
            self.scale_up(1, system_state, task_type["name"], "any")
        )

        return stop

    def create_replica(
        self, couples_suitable: Set[Tuple[Node, Platform]], task_type: TaskType
    ):
        # Scaling functions that do not yield values must still be Generators
        # No-op as per https://stackoverflow.com/a/68628599/9568489
        if False:
            yield

        # Select random platform
        found = random.randint(0, len(couples_suitable) - 1)
        random_couple = list(couples_suitable)[found]

        return random_couple

    def remove_replica(
        self,
        couples_suitable: Set[Tuple[Node, Platform]],
        task_type: TaskType,
        state: SystemState,
    ) -> Generator:
        # Scaling functions that do not yield values must still be Generators
        # No-op as per https://stackoverflow.com/a/68628599/9568489
        if False:
            yield

        # Array is shuffled in place...
        random.shuffle(list(couples_suitable))

        # Mark replica for removal if its task queue is empty
        # Return None if no replica can be removed
        removed_couple = next(
            (
                replica
                for replica in couples_suitable
                if not replica[1].queue.items and not replica[1].current_task
            ),
            None,
        )

        return removed_couple
