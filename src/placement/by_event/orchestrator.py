from __future__ import annotations

from simpy.core import Environment, SimTime
from simpy.events import Event, Process
from simpy.resources.store import FilterStore

from typing import Generator, List, Type, TYPE_CHECKING

from src.placement.orchestrator import BaseOrchestrator

if TYPE_CHECKING:
    from src.placement.by_event.autoscaler import EventAutoscaler
    from src.placement.by_event.scheduler import EventScheduler
    from src.placement.infrastructure import Application, Task

from src.placement.model import (
    SimulationData,
    SimulationPolicy,
    SystemState,
    TimeSeries,
)


class EventOrchestrator(BaseOrchestrator):
    def __init__(
        self,
        env: Environment,
        data: SimulationData,
        policy: SimulationPolicy,
        autoscaler: Type[EventAutoscaler],
        scheduler: Type[EventScheduler],
        time_series: TimeSeries,
        nodes: FilterStore,
        end_event: Event,
    ):
        self.env = env
        self.data = data
        self.policy = policy

        self.time_series = time_series
        self.nodes = nodes

        self.state: SystemState
        self.gateway: Process
        self.monitor: Process
        self.autoscaler: EventAutoscaler
        self.scheduler: EventScheduler

        self.initializer = env.process(self.initializer_process(autoscaler, scheduler))

        self.end_event = end_event
        self.end_time: SimTime

        self.application_archive: List[Application] = []
        self.task_archive: List[Task] = []

    def initializer_process(
        self, autoscaler: Type[EventAutoscaler], scheduler: Type[EventScheduler]
    ) -> Generator:
        if False:
            yield

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
