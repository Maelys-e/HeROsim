from __future__ import annotations

import logging
import math
import random

from typing import Generator, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform

from src.placement.by_event.autoscaler import EventAutoscaler
from src.placement.model import (
    DurationSecond,
    PlatformVector,
    SizeGigabyte,
    SpeedMBps,
    TaskType,
)

from src.policy.simple.model import SimpleSystemState


class SimpleAutoscaler(EventAutoscaler):
    def create_first_replica(
        self, system_state: SimpleSystemState, task_type: TaskType
    ) -> Generator:
        stop = yield self.env.process(
            self.scale_up(1, system_state, task_type["name"], "any")
        )

        return stop

    def create_replica(
        self, couples_suitable: Set[Tuple[Node, Platform]], task_type: TaskType
    ) -> Generator:
        # Scaling functions that do not yield values must still be Generators
        # No-op as per https://stackoverflow.com/a/68628599/9568489
        if False:
            yield

        # Select random platform
        found = random.randint(0, len(couples_suitable) - 1)
        random_couple = list(couples_suitable)[found]

        return random_couple

    def is_cached(
        self,
        new_replica: Tuple[Node, Platform],
        task_type: TaskType,
    ) -> bool:
        node: Node = new_replica[0]
        platform: Platform = new_replica[1]

        # Check node RAM cache
        warm_function: bool = (
            platform.previous_task is not None
            and platform.previous_task.type["name"] == task_type["name"]
        )

        return warm_function

    # TODO
    def initialize_replica(
        self,
        new_replica: Tuple[Node, Platform],
        task_type: TaskType,
        system_state: SimpleSystemState,
    ) -> Generator:
        node: Node = new_replica[0]
        platform: Platform = new_replica[1]

        retrieval_duration: DurationSecond = yield self.env.process(
            self.retrieve_function(new_replica, task_type, system_state)
        )

        # print(f"retrieval duration = {retrieval_duration}")

        # Update state

        # Retrieve function image
        yield self.env.timeout(retrieval_duration)

        # Update platform time spent on storage
        platform.storage_time += retrieval_duration

        # FIXME: Double initialize bug...
        try:
            # Set platform to ready state
            yield platform.initialized.succeed()
        except RuntimeError:
            logging.error(
                f"[ {self.env.now} ] Autoscaler tried to initialize "
                f"{new_replica[1]} ({new_replica[0]}) but it was already initialized."
            )

            logging.error(
                f"[ {self.env.now} ] Last allocation time: "
                f"{new_replica[1].last_allocated} "
                " -- Last removal time: "
                f"{new_replica[1].last_removed}"
            )

        # Statistics (Node)
        node.cache_hits += retrieval_duration == 0.0

    def remove_replica(
        self,
        couples_suitable: Set[Tuple[Node, Platform]],
        task_type: TaskType,
        state: SimpleSystemState,
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
