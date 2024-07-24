from __future__ import annotations

import functools
import logging
import math

from typing import TYPE_CHECKING, Dict, Generator, List, Set, Tuple, Type

from src.placement.model import DurationSecond, PlatformVector

from src.policy.simple.autoscaler import SimpleAutoscaler
from src.policy.simple.model import SimpleSchedulerState, SimpleSystemState
from src.policy.simple.scheduler import SimpleScheduler

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Task

from src.placement.by_event.orchestrator import EventOrchestrator


class SimpleOrchestrator(EventOrchestrator):
    autoscaler: SimpleAutoscaler
    scheduler: SimpleScheduler
    state: SimpleSystemState

    def initializer_process(
        self, autoscaler: Type[SimpleAutoscaler], scheduler: Type[SimpleScheduler]
    ) -> Generator:
        # Initialize shared data structures according to simulation policy
        self.state = self.initialize_state()

        # Initialize orchestrator components
        self.autoscaler = autoscaler(self.env, self.state, self.data, self.policy)
        self.scheduler = scheduler(
            self.env, self.state, self.data, self.policy, self.autoscaler, self.nodes
        )

        # Begin orchestration
        self.gateway = self.env.process(self.gateway_process())
        self.scheduler.run = self.env.process(self.scheduler.scheduler_process())

        # Wait for episode completion
        yield self.env.all_of([task.done for task in self.task_archive])

    def initialize_state(self) -> SimpleSystemState:
        # Initialize scheduler state
        scheduler_state = SimpleSchedulerState(
            margins={task_type: {} for task_type in self.data.task_types},
            normalized_margins={task_type: {} for task_type in self.data.task_types},
            rate_of_change={},
            low_margin_threshold=0.0,
            high_margin_threshold=0.0,
        )
        # Initialize available resources to all Tuple[Node, Platform]
        available_resources: Dict[Node, Set[Platform]] = {
            node: {platform for platform in set(node.platforms.items)}
            for node in set(self.nodes.items)
        }
        # Initialize function replicas to empty sets
        replicas: Dict[str, Set[Tuple[Node, Platform]]] = {
            task_type: set() for task_type in self.data.task_types
        }
        system_state = SimpleSystemState(
            scheduler_state=scheduler_state,
            available_resources=available_resources,
            replicas=replicas,
        )

        return system_state

    def handle_workload_event(self, task: Task) -> Generator:
        if False:
            yield

        system_state: SimpleSystemState = self.state
        state: SimpleSchedulerState = system_state.scheduler_state

        # TODO
        replicas = system_state.replicas[task.type["name"]]

        # TODO: Get current margins
        margins_sum = functools.reduce(
            lambda result, replica: result
            + state.normalized_margins[task.type["name"]][replica],
            state.normalized_margins[task.type["name"]],
            0.0,
        )

        # TODO: Get current rate of change
        if task.type["name"] not in state.rate_of_change:
            (current_roc_time, current_roc) = (self.env.now, 0.0)
        else:
            (current_roc_time, current_roc) = state.rate_of_change[task.type["name"]]

        coeff = (current_roc / 100) + 1

        # TODO: Estimate future margin @ CST
        future_margin = margins_sum * coeff

        # TODO: Check for available resources for potential new replicas
        available_replicas: List[Tuple[Node, Platform]] = [
            (a_node, a_platform)
            for a_node in system_state.available_resources
            for a_platform in system_state.available_resources[a_node]
        ]

        # TODO: Autoscaling decision according to prediction @ CST
        # low ~ 0 ; high >>> 0
        if future_margin < state.low_margin_threshold and available_replicas:
            self.autoscaler.scale_up(1, system_state, task.type["name"], "any")
        elif future_margin > state.high_margin_threshold:
            self.autoscaler.scale_down(1, system_state, task.type["name"], "any")
        else:
            pass

        # TODO: Adjust thresholds according to QoS violations
        # TODO: Check penalties across finished tasks
        # FIXME: Maybe introduce a history instead of computing durations

        # FIXME: .penalty is set only at the end of an episode!!!

        penalties = functools.reduce(
            lambda result, q_task: (result + q_task.application.penalty),
            # if q_task.finished and q_task.done_time > self.env.now - current_roc_time
            # else result + 0,
            self.task_archive,
            0,
        )

        print(penalties)

        # TODO: Historize and plot at the end
        # FIXME: Penalties threshold
        # FIXME: Increment value
        if penalties > 100:
            state.low_margin_threshold -= 1
        elif penalties < 100:
            state.high_margin_threshold += 1
        else:
            pass
