from __future__ import annotations

import functools
import logging
import math

from typing import Dict, List, Set, Tuple, TYPE_CHECKING

from src.policy.simple.model import SimpleSchedulerState, SimpleSystemState

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Task

from src.placement.by_event.scheduler import EventScheduler

from src.placement.model import DurationSecond

# from src.policy.simple.model import SimpleSystemState


class SimpleScheduler(EventScheduler):
    def placement(self, system_state: SimpleSystemState, task: Task):
        # Scheduling functions called in a Simpy Process must be Generators
        # No-op as per https://stackoverflow.com/a/68628599/9568489
        if False:
            yield

        state: SimpleSchedulerState = system_state.scheduler_state
        replicas: Set[Tuple[Node, Platform]] = system_state.replicas[task.type["name"]]

        replicas_margin: Dict[Tuple[Node, Platform], DurationSecond] = {}
        replicas_normalized_margin: Dict[Tuple[Node, Platform], DurationSecond] = {}

        for node, platform in replicas:
            # Compute task margin for heterogeneous replicas
            task_budget: DurationSecond = (
                task.type["executionTime"][platform.type["shortName"]]
                * task.application.qos["maxDurationDeviation"]
            )

            # Current task remaining time
            # + Queue remaining time
            replica_q_tasks: List[Task] = platform.queue.items
            replica_q_length: DurationSecond = (
                (
                    platform.current_task.type["executionTime"][
                        platform.type["shortName"]
                    ]
                    - (self.env.now - platform.current_task.arrived_time)
                )
                if platform.current_task is not None
                else 0.0
            ) + functools.reduce(
                lambda result, q_task: (
                    result + q_task.type["executionTime"][platform.type["shortName"]]
                ),
                replica_q_tasks,
                0.0,
            )

            # FIXME: Margins are stored according to the request QoS
            new_margin = task_budget - (
                replica_q_length
                + task.type["executionTime"][platform.type["shortName"]]
            )
            # FIXME: Margins are stored for average QoS
            new_normalized_margin = (
                task_budget / task.application.qos["maxDurationDeviation"]
            ) * self.data.qos_types["medium"]["maxDurationDeviation"] - (
                replica_q_length
                + task.type["executionTime"][platform.type["shortName"]]
            )

            # TODO: Store temporary values
            replicas_margin[(node, platform)] = new_margin
            replicas_normalized_margin[(node, platform)] = new_normalized_margin

        # TODO: Replica selection based on margin function
        if all(value < 0.0 for value in replicas_margin.values()):
            selected_replica = max(
                replicas_margin, key=lambda replica: replicas_margin[replica]
            )
        else:
            selected_replica = min(
                replicas_margin,
                key=lambda replica: replicas_margin[replica]
                if replicas_margin[replica] >= 0
                else math.inf,
            )

        # TODO: Retrieve current margins
        if (selected_replica[0].id, selected_replica[1].id) not in state.margins[
            task.type["name"]
        ]:
            state.margins[task.type["name"]][
                (selected_replica[0].id, selected_replica[1].id)
            ] = new_margin

        if (
            selected_replica[0].id,
            selected_replica[1].id,
        ) not in state.normalized_margins[task.type["name"]]:
            state.normalized_margins[task.type["name"]][
                (selected_replica[0].id, selected_replica[1].id)
            ] = new_normalized_margin

        previous_margin = state.margins[task.type["name"]][
            (selected_replica[0].id, selected_replica[1].id)
        ]
        previous_normalized_margin = state.normalized_margins[task.type["name"]][
            (selected_replica[0].id, selected_replica[1].id)
        ]

        # TODO: Store updated margins for selected replica
        state.margins[task.type["name"]][
            (selected_replica[0].id, selected_replica[1].id)
        ] = replicas_margin[selected_replica]
        state.normalized_margins[task.type["name"]][
            (selected_replica[0].id, selected_replica[1].id)
        ] = replicas_normalized_margin[selected_replica]

        # TODO: Update rate of change
        # TODO: Sum margins across replicas
        replicas_normalized_margin_sum = functools.reduce(
            lambda result, replica: result
            + state.normalized_margins[task.type["name"]][replica],
            state.normalized_margins[task.type["name"]],
            0.0,
        )

        # FIXME: Sample once every CST seconds
        if task.type["name"] not in state.rate_of_change:
            state.rate_of_change[task.type["name"]] = (self.env.now, 0.0)

        (previous_roc_time, previous_roc) = state.rate_of_change[task.type["name"]]

        # FIXME!!! Division by zero
        current_roc = (
            (new_normalized_margin - previous_normalized_margin)
            / (previous_normalized_margin if previous_normalized_margin != 0.0 else 1)
        ) * 100

        considered_cst = task.type["coldStartDuration"][
            selected_replica[1].type["shortName"]
        ]
        if self.env.now - previous_roc_time >= considered_cst:
            state.rate_of_change[task.type["name"]] = (self.env.now, current_roc)

        return selected_replica
