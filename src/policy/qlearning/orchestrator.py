from __future__ import annotations

import json
import logging
import os
import random

import numpy as np
import numpy.typing as npt

from dataclasses import dataclass

from typing import TYPE_CHECKING, Dict, Generator, Set, Tuple, Type
from enum import IntEnum

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform, Task

from src.placement.by_event.orchestrator import EventOrchestrator
from src.policy.qlearning.autoscaler import QLearningAutoscaler
from src.policy.qlearning.scheduler import QLearningScheduler
from src.policy.qlearning.model import (
    NumpyArrayJSONDecoder,
    NumpyArrayJSONEncoder,
    QLearningSchedulerState,
    QLearningSystemState,
)

from src.placement.model import SimulationData, SystemState, TaskType


"""

Autoscaling state space:

    |    | N1,P1     | N1,P2   | N2,P1        | Nn,Pp     |
    |----|-----------|---------|--------------|-----------|
    | f1 | f1r1      |         |              |           |
    | f2 |           | f2r1    | f2r2         |           |
    | f3 |           |         |              |           |
    | ff |           |         |              | ffrr      |

ffrr: should encode all useful values
    * Queue length
    * Platform metadata
    * Application metadata
    * Function metadata

Scheduling state space:

    * Gateway queue
        * Length (task count)
        * Next task (type, QoS)

    * Replica queues
        * Length (execution time)

Action space:
    * 0 = Autoscaler Select (Node, Platform) for new replica
        * Input: Function type, Hardware type
        * Output: (Node, Platform)
    * 1 = Autoscaler Select (Node, Platform) for replica removal
        * Input: Function type, Hardware type
        * Output: (Node, Platform)
    * 2 = Autoscaler NOP
    * 3 = Scheduler (Select heuristic for?) task selection
        * Input: Gateway task queue
            * Application type
            * Function type
            * QoS level
        * Output: Task
    * 4 = Scheduler Select replica
        * Input: System state, Function type, Application type, QoS level
        * Output: (Node, Platform)
    * 5 = Scheduler NOP

Reward function:
    * 

"""


class QLearningAction(IntEnum):
    SCHEDULE = 0


@dataclass
class QLearningParams:
    function: TaskType
    node: Node
    platform: Platform
    task: Task


class QLearningAgent:
    def __init__(
        self,
        data: SimulationData,
        max_replicas: int,
        q_table: Dict[int, np.ndarray] = {},
        learning_rate: float = 0.1,
        discount_factor: float = 0.99,
        exploration_rate: float = 1.0,
        exploration_decay: float = 0.99,
        min_exploration_rate: float = 0.01,
    ):
        # Action space (scheduling)
        functions_count = len(data.task_types)
        num_actions = 1  # Scheduling only
        num_functions = functions_count
        num_replicas = max_replicas

        self.data = data

        self.num_actions: int = num_actions
        self.num_functions: int = num_functions
        self.num_replicas: int = num_replicas

        self.learning_rate: float = learning_rate
        self.discount_factor: float = discount_factor
        self.exploration_rate: float = exploration_rate
        self.exploration_decay: float = exploration_decay
        self.min_exploration_rate: float = min_exploration_rate

        # Initialize Q-table with initial state
        self.q_table: Dict[int, npt.NDArray] = q_table

    def choose_action(
        self, system_state: QLearningSystemState, task: Task
    ) -> Tuple[QLearningAction, QLearningParams]:
        # TODO: Determine a set of legal actions depending on current system state

        # Hash system state for Q-Table lookup
        state = hash(system_state)

        if state not in self.q_table:
            # TODO: For now!
            # x = actions, y = functions, z = replicas
            # q_table[state][x][y][z] = q_value
            """
            >>> x = numpy.zeros((2, 2, 3))
            >>> x
            array([[[0., 0., 0.],
                    [0., 0., 0.]],

                [[0., 0., 0.],
                    [0., 0., 0.]]])
            """
            self.q_table[state] = np.zeros(
                (self.num_actions, self.num_functions, self.num_replicas)
            )
            with open(
                os.path.join("data", "qlearning", "qtable.json"), "w+"
            ) as outfile:
                json.dump(self.q_table, outfile, cls=NumpyArrayJSONEncoder)

        if np.random.uniform(0, 1) < self.exploration_rate:
            # Exploration
            random_function = random.choice(tuple(self.data.task_types.values()))
            random_node, random_platform = random.choice(
                tuple(system_state.replicas[random_function["name"]])
            )

            return (
                # QLearningAction(np.random.choice(self.num_actions)),
                QLearningAction(0),  # Scheduling only
                QLearningParams(
                    function=random_function,
                    node=random_node,
                    platform=random_platform,
                    task=task,
                ),
            )
        else:
            # Exploitation
            i_action: int
            i_function: int
            i_replica: int
            # [i_action, i_function, i_replica] = np.argwhere(self.q_table[state] == np.max(self.q_table[state]))
            [i_action, i_function, i_replica] = np.unravel_index(
                np.argmax(self.q_table[state]), self.q_table[state].shape
            )

            q_function = list(self.data.task_types.values())[i_function]
            q_node, q_platform = list(system_state.replicas[q_function["name"]])[
                i_replica
            ]

            return (
                # QLearningAction(i_action),
                QLearningAction(0),  # Scheduling only
                QLearningParams(
                    function=q_function, node=q_node, platform=q_platform, task=task
                ),
            )

    def update_q_table(
        self,
        h_system_state: int,
        action: QLearningAction,
        reward: float,
        h_next_state: int,
    ) -> None:
        if h_next_state not in self.q_table:
            self.q_table[h_next_state] = np.zeros(
                (self.num_actions, self.num_functions, self.num_replicas)
            )

        current_q_value: float = self.q_table[h_system_state][action]
        max_future_q_value: float = np.max(self.q_table[h_next_state])
        new_q_value: float = (
            1 - self.learning_rate
        ) * current_q_value + self.learning_rate * (
            reward + self.discount_factor * max_future_q_value
        )

        self.q_table[h_system_state][action] = new_q_value

    def decay_exploration_rate(self) -> None:
        if self.exploration_rate > self.min_exploration_rate:
            self.exploration_rate *= self.exploration_decay


class QLearningOrchestrator(EventOrchestrator):
    autoscaler: QLearningAutoscaler
    scheduler: QLearningScheduler

    def initializer_process(
        self, autoscaler: Type[QLearningAutoscaler], scheduler: Type[QLearningScheduler]
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
        self.monitor = self.env.process(self.monitor_process())

        # Disable autoscaler and scheduler processes
        # Use Q-Learning agent instead :-)
        # self.autoscaler.run = self.env.process(self.autoscaler.autoscaler_process())
        # self.scheduler.run = self.env.process(self.scheduler.scheduler_process())

        # Initialize Q-Table
        q_table: Dict[int, np.ndarray] = {}
        # Load Q-Table if it exists on disk
        if os.path.exists(os.path.join("data", "qlearning", "qtable.json")):
            with open(os.path.join("data", "qlearning", "qtable.json")) as infile:
                q_table = json.load(infile, cls=NumpyArrayJSONDecoder)

        system_state: QLearningSystemState = self.state

        self.agent = QLearningAgent(
            data=self.data,
            max_replicas=len(system_state.available_resources.items()),
            q_table=q_table,
        )

        # Fixed number of replicas throughout the scenario
        # Deploy function f1 on all the available resources
        stop = yield self.env.process(
            self.autoscaler.scale_up(
                len(system_state.available_resources.items()), system_state, "f1", "any"
            )
        )

        # Wait for episode completion
        yield self.env.all_of([task.done for task in self.task_archive])

        # Save Q-Table to disk once episode is finished
        if not os.path.exists(os.path.join("data", "qlearning")):
            os.makedirs(os.path.join("data", "qlearning"))
        with open(os.path.join("data", "qlearning", "qtable.json"), "w") as outfile:
            json.dump(q_table, outfile, indent=2, cls=NumpyArrayJSONEncoder)

    def initialize_state(self) -> QLearningSystemState:
        scheduler_state = QLearningSchedulerState(
            target_concurrencies={
                task_type: {
                    platform: 0.0
                    for platform in self.data.task_types[task_type]["platforms"]
                }
                for task_type in self.data.task_types
            },
            average_hardware_contention={
                task_type: {
                    platform: 0.0
                    for platform in self.data.task_types[task_type]["platforms"]
                }
                for task_type in self.data.task_types
            },
            penalties={task_type: 0 for task_type in self.data.task_types},
            queues={},
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
        system_state = QLearningSystemState(
            scheduler_state=scheduler_state,
            available_resources=available_resources,
            replicas=replicas,
        )

        return system_state

    # TODO: Add event type to function call
    def handle_workload_event(self, task: Task) -> Generator:
        # TODO: Switch on event type and build the table of legal actions
        print(f"handle workload event {task.id}")
        # Determine current state
        system_state: QLearningSystemState = self.state
        # state_copy = copy.copy(system_state)
        h_system_state = hash(system_state)

        # Check Q-Table for adequate Q-Value (explore vs exploit)
        action: QLearningAction
        params: QLearningParams
        action, params = self.agent.choose_action(system_state, task)
        print(f"action = {action}, params = {params.platform}")

        # Apply action and receive reward
        reward: float
        next_state: QLearningSystemState
        reward, next_state = yield self.env.process(self.take_action(action, params))
        h_next_state = hash(next_state)

        # Update Q-Table
        self.agent.update_q_table(h_system_state, action, reward, h_next_state)

        # Converge
        self.agent.decay_exploration_rate()

    def take_action(
        self, action: QLearningAction, params: QLearningParams
    ) -> Generator:
        match action:
            case QLearningAction.SCHEDULE:
                # Schedule
                yield self.env.process(
                    self.scheduler.schedule(params.task, params.node, params.platform)
                )

                system_state: QLearningSystemState = self.state
                # TODO: Copy state!
                # state_copy = copy.copy(system_state)

                return (0.0, system_state)

    def monitor_process(self) -> Generator:
        logging.info(f"[ {self.env.now} ] Orchestrator Monitor started")

        while True:
            system_state: QLearningSystemState = self.state

            # Wake Monitor up once per second
            yield self.env.timeout(1)
