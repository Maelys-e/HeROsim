from __future__ import annotations
from abc import abstractmethod

import logging
from timeit import default_timer

from simpy.core import Environment
from simpy.events import Process
from simpy.resources.store import FilterStore

from typing import TYPE_CHECKING

from src.placement.scheduler import BaseScheduler

if TYPE_CHECKING:
    from src.placement.by_event.autoscaler import EventAutoscaler

from src.placement.model import SimulationData, SimulationPolicy, SystemState
from src.placement.resources import PriorityFilterStore


class EventScheduler(BaseScheduler):
    def __init__(
        self,
        env: Environment,
        system_state: SystemState,
        data: SimulationData,
        policy: SimulationPolicy,
        autoscaler: EventAutoscaler,
        nodes: FilterStore,
    ):
        self.env = env
        self.state = system_state
        self.data = data
        self.policy = policy
        self.autoscaler = autoscaler

        self.run: Process

        self.nodes = nodes
        self.tasks = PriorityFilterStore(env)
