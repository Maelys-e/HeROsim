from __future__ import annotations

from simpy.core import Environment
from simpy.events import Process

from typing import List

from src.placement.autoscaler import BaseAutoscaler

from src.placement.model import (
    ScaleEvent,
    SimulationData,
    SimulationPolicy,
    SystemState,
)


class EventAutoscaler(BaseAutoscaler):
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
