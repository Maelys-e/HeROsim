from __future__ import annotations

import logging
import random

from typing import Generator, Set, Tuple, TYPE_CHECKING

from src.policy.qlearning.model import QLearningSchedulerState, QLearningSystemState


if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Storage

from src.placement.model import (
    DurationSecond,
    SizeGigabyte,
    SpeedMBps,
    SystemState,
    TaskType,
)

from src.placement.by_event.autoscaler import EventAutoscaler


class QLearningAutoscaler(EventAutoscaler):
    def create_first_replica(
        self, system_state: SystemState, task_type: TaskType
    ) -> Generator:
        if False:
            yield
        pass

    def create_replica(
        self, couples_suitable: Set[Tuple[Node, Platform]], task_type: TaskType
    ) -> Generator:
        if False:
            yield

        # Select random platform
        found = random.randint(0, len(couples_suitable) - 1)
        random_couple = list(couples_suitable)[found]

        return random_couple

    def initialize_replica(
        self,
        new_replica: Tuple[Node, Platform],
        task_type: TaskType,
        system_state: QLearningSystemState,
    ) -> Generator:
        node: Node = new_replica[0]
        platform: Platform = new_replica[1]

        # Check node RAM cache
        warm_function: bool = (
            platform.previous_task is not None
            and platform.previous_task.type["name"] == task_type["name"]
        )

        # TODO: Check if function image is cached on one of the node's storage devices
        # cache_storage: Storage | None = None
        cache_storage: bool = False
        node_storage: Storage
        for node_storage in node.storage.items:
            if node_storage.has_function(platform.type["shortName"], task_type):
                cache_storage = True
                break

        # Initialize image retrieval duration
        retrieval_duration: DurationSecond = 0.0

        # TODO: Retrieve image if function not in RAM cache nor in disk cache
        # FIXME: Should be factored in superclass
        if not warm_function and not cache_storage:
            logging.info(
                f"[ {self.env.now} ] 💾 {node} needs to pull image for {task_type}"
            )

            # Update image retrieval duration
            retrieval_size: SizeGigabyte = task_type["imageSize"][
                platform.type["shortName"]
            ]
            # Depends on storage performance
            # FIXME: What's the policy for storage selection?
            node_storage = yield node.storage.get(
                lambda storage: not storage.type["remote"]
            )
            # Depends on network link speed
            retrieval_speed: SpeedMBps = min(
                node_storage.type["throughput"]["write"], node.network["bandwidth"]
            )
            retrieval_duration += (
                retrieval_size / (retrieval_speed / 1024)
                + node_storage.type["latency"]["write"]
            )

            # print(f"retrieval size = {retrieval_size}")
            # print(f"retrieval speed = {retrieval_speed}")
            # print(f"retrieval duration = {retrieval_duration}")

            # TODO: Update disk usage
            stored = node_storage.store_function(platform.type["shortName"], task_type)

            if not stored:
                logging.warning(
                    f"[ {self.env.now} ] 💾 {node_storage} has no available capacity to"
                    f" cache image for {self}"
                )

            # Proactively cache next functions
            # FIXME: Compute once and keep in SystemState
            applications_of_task: Set[str] = set()
            for application_type_name in self.data.application_types:
                for task_type_name in self.data.application_types[
                    application_type_name
                ]["dag"]:
                    if task_type_name == task_type["name"]:
                        applications_of_task.add(application_type_name)

            # List applications that include considered task type
            for application_name in applications_of_task:
                application = self.data.application_types[application_name]
                for function_name in application["dag"]:
                    function = self.data.task_types[function_name]

                    # Intersect task compatibility and node-available platforms
                    prefetch_function_platforms = set(function["platforms"])
                    prefetch_node_platforms = [
                        node_platform.type["name"]
                        for node_platform in node.platforms.items
                    ]
                    prefetch_platforms = prefetch_function_platforms.intersection(
                        prefetch_node_platforms
                    )

                    # Prefetch images for the next functions in the application
                    # FIXME: Retrieval time, timeout...
                    for prefetch_platform in prefetch_platforms:
                        stored = node_storage.store_function(
                            prefetch_platform, function
                        )

                        if not stored:
                            logging.warning(
                                f"[ {self.env.now} ] 💾 {node_storage} has no available"
                                f" capacity to cache image for {self}"
                            )

            # Release storage
            yield node.storage.put(node_storage)

        # print(f"retrieval duration = {retrieval_duration}")

        # Update state
        state: QLearningSchedulerState = system_state.scheduler_state
        # HRC policy
        state.average_hardware_contention[task_type["name"]][
            new_replica[1].type["shortName"]
        ] += 1.0
        """
        # Round Robin placement
        state.scheduled_count[task_type["name"]][
            (new_replica[0].id, new_replica[1].id)
        ] = 0
        """

        # FIXME: Retrieve function image
        yield self.env.timeout(retrieval_duration)

        # FIXME: Update platform time spent on storage
        platform.storage_time += retrieval_duration

        # FIXME: Double initialize bug...
        try:
            # Set platform to ready state
            yield platform.initialized.succeed()
        except RuntimeError:
            """
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
            """
            pass

        # Statistics (Node)
        node.cache_hits += cache_storage

    def remove_replica(
        self,
        function_replicas: Set[Tuple[Node, Platform]],
        task_type: TaskType,
        state: QLearningSystemState,
    ) -> Generator:
        if False:
            yield
        pass
