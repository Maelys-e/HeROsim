from __future__ import annotations
from abc import abstractmethod

import logging
import math

from simpy.core import Environment, SimTime
from simpy.events import Process

from typing import Dict, Generator, List, Set, Tuple, TYPE_CHECKING

from src.placement.autoscaler import BaseAutoscaler

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform

from src.placement.model import (
    PlatformVector,
    ScaleEvent,
    SimulationData,
    SimulationPolicy,
    SystemState,
    TaskType,
)


class ThresholdAutoscaler(BaseAutoscaler):
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

    @abstractmethod
    def scaling_level(
        self, system_state: SystemState, task_type: TaskType
    ) -> Generator:
        pass

    def autoscaler_process(self) -> Generator:
        logging.info(
            f"[ {self.env.now} ] Orchestrator Autoscaler started with policy"
            f" {self.policy}"
        )

        last_force_scale_up: Dict[str, SimTime] = {
            function_name: 0.0 for function_name in self.data.task_types
        }

        while True:
            # Per-function scaling decision
            system_state: SystemState = self.state
            replicas: Dict[str, Set[Tuple[Node, Platform]]] = system_state.replicas

            for function_name, function_replicas in replicas.items():
                force_scale_up = True

                scaling_difference: PlatformVector[float] = yield self.env.process(
                    self.scaling_level(
                        system_state, self.data.task_types[function_name]
                    )
                )

                for hardware_target, hardware_scaling in scaling_difference.items():
                    if hardware_scaling < 0:
                        # Scale down
                        count = abs(math.floor(hardware_scaling))
                        """
                        logging.info(
                            f"[ {self.env.now} ] Scaling down {function_name} by"
                            f" {count} (currently {len(function_replicas)})"
                        )
                        """
                        stop = yield self.env.process(
                            self.scale_down(
                                count, system_state, function_name, hardware_target
                            )
                        )
                        # Do not force scale up
                        force_scale_up = False

                    elif hardware_scaling > 0:
                        # Scale up
                        count = abs(math.ceil(hardware_scaling))
                        """
                        logging.info(
                            f"[ {self.env.now} ] Scaling up {function_name} by"
                            f" {count} (currently {len(function_replicas)})"
                        )
                        """
                        stop = yield self.env.process(
                            self.scale_up(
                                count, system_state, function_name, hardware_target
                            )
                        )
                        # Successfully scaled up on hardware target
                        if not isinstance(stop, StopIteration):
                            force_scale_up = False

                    else:
                        # Correct scaling level, do nothing
                        force_scale_up = False
                        # pass

                # Force scale up on any hardware type if necessary
                if force_scale_up and (
                    (self.env.now - last_force_scale_up[function_name])
                    > self.policy.keep_alive
                ):
                    stop = yield self.env.process(
                        self.create_first_replica(
                            system_state, self.data.task_types[function_name]
                        )
                    )
                    last_force_scale_up[function_name] = self.env.now

            # Next event
            self.env.step()

            # Wake Autoscaler up once per second
            # yield self.env.timeout(1)
