from __future__ import annotations

import logging

from typing import Generator, TYPE_CHECKING

if TYPE_CHECKING:
    from src.placement.infrastructure import Task, Node, Platform


from src.placement.scheduler import BaseScheduler


class QLearningScheduler(BaseScheduler):
    def select(self) -> Generator:
        # TODO: Get tasks for which dependencies are satisfied (finished events)
        # Scheduler will consider tasks with satisfied dependencies,
        # then select according to task priority policy
        # See src.placement.resources.PriorityFilterStore for queue management
        # See Task.__lt__ for selection policies (i.e. EDF and FIFO)
        task: Task = yield self.tasks.get(
            lambda queued_task: all(
                dependency.finished for dependency in queued_task.dependencies
            )
        )

        return task

    def schedule(
        self, task: Task, sched_node: Node, sched_platform: Platform
    ) -> Generator:
        logging.info(f"[ {self.env.now} ] Scheduler woken up")

        # Remove task from scheduler queue
        queued_task: Task = yield self.tasks.get(
            lambda queued_task: queued_task.id == task.id
        )

        # Update node
        node: Node = yield self.nodes.get(lambda node: node.id == sched_node.id)
        task.node = node
        node.unused = False
        # Update platform
        platform: Platform = yield node.platforms.get(
            lambda platform: platform.id == sched_platform.id
        )
        task.platform = platform

        # TODO: Node cache management?

        yield platform.queue.put(task)
        yield task.scheduled.succeed()

        # Release platform
        yield node.platforms.put(platform)

        # Node is released
        yield self.nodes.put(node)
