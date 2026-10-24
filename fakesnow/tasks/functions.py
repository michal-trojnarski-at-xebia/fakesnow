"""What a task body can know about the graph run it is part of."""

from __future__ import annotations

from dataclasses import dataclass

from fakesnow.tasks.catalog import Task


@dataclass
class TaskRunContext:
    """The run of one task in a graph run, set on the task's session while its body runs."""

    task: Task
    root: Task
    graph_run_group_id: str
    run_id: int
    config: str | None
    predecessor_return_values: dict[str, str | None]
    return_value: str | None = None
