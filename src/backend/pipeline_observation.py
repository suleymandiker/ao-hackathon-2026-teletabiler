"""Optional synchronous observation of borrowed stage outputs.

Observers must not mutate inputs. They belong to one invocation, never learning
state. Processing layers know this callback only, not the monitoring read model.
"""
from typing import Any, Literal, Protocol

Observation = Literal['segmentation', 'parsing', 'patterns', 'membership', 'expert_input']


class PipelineObserver(Protocol):
    def __call__(self, stage: Observation, value: Any) -> None: ...
