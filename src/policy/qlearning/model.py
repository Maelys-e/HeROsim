from __future__ import annotations

from json import JSONEncoder, JSONDecoder

from dataclasses import dataclass
from typing import Dict, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.placement.infrastructure import Node, Platform

import numpy

from src.placement.model import PlatformVector, SchedulerState, SystemState


class NumpyArrayJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return super().default(obj)


class NumpyArrayJSONDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.dict_to_obj, *args, **kwargs)

    def dict_to_obj(self, dct):
        for key, value in dct.items():
            if isinstance(value, list):
                try:
                    dct[key] = numpy.array(value)
                except ValueError:
                    pass
        return dct


@dataclass
class QLearningSchedulerState(SchedulerState):
    average_hardware_contention: Dict[str, PlatformVector[float]]
    penalties: Dict[str, int]
    queues: Dict[Tuple[Node, Platform], int]


@dataclass
class QLearningSystemState(SystemState):
    scheduler_state: QLearningSchedulerState

    def __eq__(self, other: QLearningSystemState) -> bool:
        if len(self.replicas) != len(other.replicas):
            return False

        if self.replicas.keys() != other.replicas.keys():
            return False

        for function, replicas in self.replicas.items():
            if len(replicas) != len(other.replicas[function]):
                return False

            if set([node.id for node, _ in replicas]) != set(
                [node.id for node, _ in other.replicas[function]]
            ):
                return False

            if set([platform.id for _, platform in replicas]) != set(
                [platform.id for _, platform in other.replicas[function]]
            ):
                return False

            for i in range(len(replicas)):
                if list(replicas)[i] != list(other.replicas[function])[i]:
                    return False

        return True

    """
    def __eq__(self, other: QLearningSystemState) -> bool:
        if len(self.available_resources) != len(other.available_resources):
            return False
        
        if len(self.replicas) != len(other.replicas):
            return False
        
        if len(self.scheduler_state.average_hardware_contention) != len(other.scheduler_state.average_hardware_contention):
            return False
        
        # TODO: Deep equals
        if set(self.available_resources.items()) != set(other.available_resources.items()):
            return False
        
        # TODO: Deep equals
        if set(self.replicas.items()) != set(other.replicas.items()):
            return False
        
        # TODO: Deep equals
        if set(self.scheduler_state.average_hardware_contention.items()) != set(other.scheduler_state.average_hardware_contention.items()):
            return False
        
        return True
    """

    # FIXME: Hic sunt dracones
    def __hash__(self) -> int:
        h_resources = tuple(
            [
                hash(tuple([hash(node), hash(tuple(platforms))]))
                for node, platforms in self.available_resources.items()
            ]
        )

        h_replicas = tuple(
            [
                hash(tuple([hash(function), hash(tuple(replicas))]))
                for function, replicas in self.replicas.items()
            ]
        )

        h_contentions = tuple(
            [
                hash(tuple([hash(function), hash(tuple(contentions))]))
                for function, contentions in self.scheduler_state.average_hardware_contention.items()
            ]
        )

        return hash(tuple([h_resources, h_replicas, h_contentions]))
