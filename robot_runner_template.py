from pathlib import Path

from prefect import Flow, task
from prefect.client import Secret
from prefect.storage import Git
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

### PARAMETERS

# Parameters of this flow
name = Path(__file__).stem
project_name = "pix_robots"
labels = ["prefect"]

# Dependent flow parameters
run_pix_name = "run_pix"
run_pix_project_name = "pix_robots"
parameters = {
    "script_path": r"\\oao\data\.ОАО Сити-Сервис\02 Экономический отдел\RPA\task.pix",
    "script_parameters": {"time": 10},
    "robot_path": None,
    "need_gui": True,
}

### RUN FLOW FROM FLOW

# Script location in Git
work_dir = Path().absolute()
module_path = Path(__file__)
relative_path = module_path.relative_to(work_dir)

storage = Git(
    repo=Secret("GIT_REPO").get(),
    flow_path=relative_path.as_posix(),
    repo_host=Secret("GIT_SERVER_HOST").get(),
    branch_name=Secret("GIT_BRANCH").get(),
)


with Flow(f"STARTER_{name}", storage=storage) as flow:
    flow_child = create_flow_run(
        flow_name=run_pix_name,
        project_name=run_pix_project_name,
        parameters=parameters,
        run_name=f"RUNNER_{name}",
    )
    wait_for_flow_child = wait_for_flow_run(flow_child, raise_final_state=True)


flow.register(
    project_name=project_name,
    labels=labels,
    add_default_labels=False,
)
