from test import parameters
from prefect import task, Flow
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run,
    get_task_run_result,
)


name = "test_task"
run_pix_name = "run_pix"
run_pix_project_name = "pix_robots"
parameters = {
    "script_path": r"\\oao\data\.ОАО Сити-Сервис\02 Экономический отдел\RPA\task.pix",
    "script_parameters": {"time": 10},
    "robot_path": None,
    "need_gui": True,
}

with Flow(f"STARTER_{name}") as flow:
    flow_child = create_flow_run(
        flow_name=run_pix_name,
        project_name=run_pix_project_name,
        parameters=parameters,
        run_name=f"RUNNER_{name}",
    )
    wait_for_flow_child = wait_for_flow_run(flow_child, raise_final_state=True)


flow.register(project_name="pix_robots")
