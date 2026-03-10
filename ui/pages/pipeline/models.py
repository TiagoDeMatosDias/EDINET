from dataclasses import dataclass


@dataclass
class PipelinePageState:
    env: dict[str, str]
    app_state: dict
    run_cfg: dict
    steps: list[list]
    step_configs: dict[str, dict]
    is_running: list[bool]
