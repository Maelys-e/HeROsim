from __future__ import annotations
from abc import abstractmethod

import logging

from simpy.core import Environment
from simpy.events import Process

from typing import Any, Dict, Generator, List, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Storage

from src.placement.model import (
    DurationSecond,
    ScaleEvent,
    SimulationData,
    SimulationPolicy,
    SizeGigabyte,
    SpeedMBps,
    SystemState,
    TaskType,
)


class BaseAutoscaler:
    def __init__(
        self,
        env: Environment,
        system_state: SystemState,
        data: SimulationData,
        policy: SimulationPolicy,
    ):
        self.env = env
        self.state = system_state
        self.data = data
        self.policy = policy

        self.scale_events: List[ScaleEvent] = []

        self.run: Process

    def scale_up(
        self,
        count: int,
        system_state: SystemState,
        function_name: str,
        hardware_target: str,
    ) -> Generator:
        # Get current function replicas
        function_replicas = system_state.replicas[function_name]

        # Scale up by `count` replicas
        for _ in range(count):
            replicas_count = len(function_replicas)
            # Filter out nodes by task requirements
            couples_suitable: Set[Tuple[Node, Platform]] = set()

            available_resources: Dict[
                Node, Set[Platform]
            ] = system_state.available_resources
            for node, platforms in available_resources.items():
                for platform in platforms:
                    if (
                        hardware_target != "any"
                        and platform.type["shortName"] != hardware_target
                    ):
                        continue
                    if (
                        platform.type["shortName"]
                        not in self.data.task_types[function_name]["platforms"]
                    ):
                        continue
                    if (
                        node.memory
                        < self.data.task_types[function_name]["memoryRequirements"][
                            platform.type["shortName"]
                        ]
                    ):
                        continue
                    couples_suitable.add((node, platform))

            # No suitable resources for replica creation
            if not couples_suitable:
                # logging.error(state.average_hardware_contention[function_name])
                # Next step
                return StopIteration(
                    f"Autoscaler could not create a {hardware_target} replica for"
                    f" {function_name} (currently {replicas_count} replica)"
                )

            logging.info(
                f"[ {self.env.now} ] Autoscaler scaling up {function_name} (currently"
                f" {replicas_count})"
            )

            # Resources selection (Node, Platform)
            new_replica: Tuple[Node, Platform]
            new_replica = yield self.env.process(
                self.create_replica(
                    couples_suitable, self.data.task_types[function_name]
                )
            )

            logging.info(f"[ {self.env.now} ] {new_replica}")

            try:
                # Remove selected platform from available resources on the node
                available_resources[new_replica[0]].remove(new_replica[1])

                # Update node availability
                new_replica[0].available_platforms -= 1

                # Allocate task memory requirements from node's available memory
                new_replica[0].available_memory -= self.data.task_types[function_name][
                    "memoryRequirements"
                ][new_replica[1].type["shortName"]]

                # Add function replica to the pool, so it can be considered by the Scheduler
                function_replicas.add(new_replica)

                # Initialize replica (pull image)
                # It will be available for task execution when function image is pulled
                self.env.process(
                    self.initialize_replica(
                        new_replica,
                        self.data.task_types[function_name],
                        system_state,
                    )
                )

                # Statistics
                new_replica[1].last_allocated = self.env.now

                event: ScaleEvent = {
                    "name": function_name,
                    "timestamp": self.env.now,
                    "action": "up",
                    "count": len(function_replicas),
                    "average_queue_length": sum(
                        [len(replica[1].queue.items) for replica in function_replicas]
                    )
                    / len(function_replicas),
                }
                self.scale_events.append(event)
            except KeyError:
                """
                logging.error(
                    f"[ {self.env.now} ] Autoscaler tried to scale up "
                    f"{function_name}, but {new_replica} was already allocated"
                )

                logging.error(
                    f"[ {self.env.now} ] Last allocation time: "
                    f"{new_replica[1].last_allocated} "
                    " -- Last removal time: "
                    f"{new_replica[1].last_removed}"
                )

                logging.error(
                    f"[ {self.env.now} ] {system_state.available_resources}"
                )
                logging.error(
                    f"{new_replica[1].initialized} // {new_replica[0].available_platforms}"
                )
                """
                pass

    def scale_down(
        self,
        count: int,
        system_state: SystemState,
        function_name: str,
        hardware_target: str,
    ):
        # Get current function replicas
        function_replicas = system_state.replicas[function_name]

        # Filter replicas according to hardware target
        suitable_replicas = set(
            filter(
                lambda replica: replica[1].type["shortName"] == hardware_target,
                function_replicas,
            )
        )

        # Scale down
        for _ in range(count):
            replicas_count = len(function_replicas)

            removed_replica: Tuple[Node, Platform]
            removed_replica = yield self.env.process(
                self.remove_replica(
                    suitable_replicas, self.data.task_types[function_name], system_state
                )
            )

            # Could not scale down (tasks in queue on all replicas)
            if not removed_replica:
                # Next step
                return StopIteration(
                    f"Autoscaler could not scale down {function_name} (currently"
                    f" {replicas_count})"
                )

            logging.info(
                f"[ {self.env.now} ] Autoscaler scaling down {function_name} (currently"
                f" {replicas_count})"
            )

            logging.info(f"[ {self.env.now} ] {removed_replica}")

            try:
                # Remove replica from function replicas
                # FIXME: Sometimes raises KeyError ... (double remove)
                function_replicas.remove(removed_replica)

                # Reset platform to uninitialized state
                removed_replica[1].initialized = removed_replica[1].env.event()

                # Release replica into available resources
                available_resources: Dict[
                    Node, Set[Platform]
                ] = system_state.available_resources
                available_resources[removed_replica[0]].add(removed_replica[1])

                # Update node availability
                removed_replica[0].available_platforms += 1

                # Reclaim node memory
                removed_replica[0].available_memory += self.data.task_types[
                    function_name
                ]["memoryRequirements"][removed_replica[1].type["shortName"]]

                # Statistics
                removed_replica[1].last_removed = self.env.now

                event: ScaleEvent = {
                    "name": function_name,
                    "timestamp": self.env.now,
                    "action": "down",
                    "count": len(function_replicas),
                    "average_queue_length": (
                        sum(
                            [
                                len(replica[1].queue.items)
                                for replica in function_replicas
                            ]
                        )
                        / len(function_replicas)
                        if function_replicas
                        else 0.0
                    ),
                }
                self.scale_events.append(event)
            except KeyError:
                """
                logging.error(
                    f"[ {self.env.now} ] Autoscaler tried to scale down "
                    f"{function_name}, but {removed_replica} was already removed"
                )

                logging.error(
                    f"[ {self.env.now} ] Last allocation time: "
                    f"{removed_replica[1].last_allocated} "
                    " -- Last removal time: "
                    f"{removed_replica[1].last_removed}"
                )

                logging.error(
                    f"[ {self.env.now} ] {system_state.available_resources}"
                )
                logging.error(
                    f"{removed_replica[1].initialized} // {removed_replica[0].available_platforms}"
                )
                """
                pass

    @abstractmethod
    def create_first_replica(
        self, system_state: SystemState, task_type: TaskType
    ) -> Generator:
        pass

    @abstractmethod
    def create_replica(
        self, couples_suitable: Set[Tuple[Node, Platform]], task_type: TaskType
    ) -> Generator:
        pass

    @abstractmethod
    def is_cached(
        self,
        new_replica: Tuple[Node, Platform],
        task_type: TaskType,
    ) -> bool:
        pass

    def retrieve_function(
        self,
        new_replica: Tuple[Node, Platform],
        task_type: TaskType,
        system_state: SystemState,
    ) -> Generator[Any, Any, float]:
        node: Node = new_replica[0]
        platform: Platform = new_replica[1]

        # Check node cache for function image availability
        is_cached: bool = self.is_cached(new_replica, task_type)

        # Initialize image retrieval duration
        retrieval_duration: DurationSecond = 0.0

        # Retrieve image if function is not in node cache
        if not is_cached:
            logging.info(
                f"[ {self.env.now} ] 💾 {node} needs to pull image for {task_type}"
            )

            # Update image retrieval duration
            retrieval_size: SizeGigabyte = task_type["imageSize"][
                platform.type["shortName"]
            ]
            # Depends on storage performance
            # FIXME: What's the policy for storage selection?
            node_storage: Storage
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

            # TODO: Update disk usage
            stored = node_storage.store_function(platform.type["shortName"], task_type)

            if not stored:
                logging.error(
                    f"[ {self.env.now} ] 💾 {node_storage} has no available capacity to"
                    f" cache image for {self}"
                )

            # TODO: Post retrieval operations
            yield self.env.process(
                self.post_retrieve_function(
                    new_replica, task_type, node_storage, system_state
                )
            )

            # Release storage
            yield node.storage.put(node_storage)

        # print(f"retrieval duration = {retrieval_duration}")

        return retrieval_duration

    @abstractmethod
    def post_retrieve_function(
        self,
        new_replica: Tuple[Node, Platform],
        task_type: TaskType,
        node_storage: Storage,
        system_state: SystemState,
    ) -> Generator:
        # FIXME: There might be a semantically better no-op to use here
        yield self.env.timeout(0)

    @abstractmethod
    def initialize_replica(
        self,
        new_replica: Tuple[Node, Platform],
        task_type: TaskType,
        system_state: SystemState,
    ) -> Generator:
        pass

    @abstractmethod
    def remove_replica(
        self,
        couples_suitable: Set[Tuple[Node, Platform]],
        task_type: TaskType,
        state: SystemState,
    ) -> Generator:
        pass
