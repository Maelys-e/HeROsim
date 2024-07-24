from dataclasses import dataclass
from typing import Dict, List, Tuple

from src.placement.model import (
    DurationSecond,
    MomentSecond,
    SchedulerState,
    SystemState,
)


@dataclass
class SimpleSchedulerState(SchedulerState):
    margins: Dict[str, Dict[Tuple[int, int], DurationSecond]]
    normalized_margins: Dict[str, Dict[Tuple[int, int], DurationSecond]]
    rate_of_change: Dict[str, Tuple[MomentSecond, DurationSecond]]
    # FIXME: Move to SystemState?
    low_margin_threshold: DurationSecond
    high_margin_threshold: DurationSecond


@dataclass
class SimpleSystemState(SystemState):
    scheduler_state: SimpleSchedulerState
