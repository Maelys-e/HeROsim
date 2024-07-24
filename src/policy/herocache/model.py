from dataclasses import dataclass
from typing import Dict

from src.placement.model import PlatformVector, SystemState, ThresholdSchedulerState


@dataclass
class HRCSchedulerState(ThresholdSchedulerState):
    average_hardware_contention: Dict[str, PlatformVector]


@dataclass
class HRCSystemState(SystemState):
    scheduler_state: HRCSchedulerState
