from dataclasses import dataclass
from typing import Dict, Tuple

from src.placement.model import SystemState, ThresholdSchedulerState


@dataclass
class KnativeSchedulerState(ThresholdSchedulerState):
    average_contention: Dict[str, Dict[Tuple[int, int], float]]
    panic_contention: Dict[str, Dict[Tuple[int, int], float]]


@dataclass
class KnativeSystemState(SystemState):
    scheduler_state: KnativeSchedulerState
