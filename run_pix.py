import json
import re
import subprocess
from pathlib import Path
from typing import Any, Optional, Union

import prefect
from prefect import Flow, Parameter, case, task
from prefect.client import Secret
from prefect.executors import DaskExecutor
from prefect.storage import Git


@task(tags=["dask-resource:pix=1"], log_stdout=True)
def run_cmd(cmd: list[str]) -> int:
    """run command string in terminal

    Args:
        cmd (list[str]): command string list for execution

    Raises:
        RuntimeError: error if exit code != 0

    Returns:
        int: exit code
    """
    logger = prefect.context.get("logger")
    logger.info(f"run {cmd=}")

    p = subprocess.run(cmd, capture_output=True, encoding="cp866")

    logger.info(p.stdout)
    logger.error(p.stderr)

    exit_code = p.returncode

    if exit_code != 0:
        raise RuntimeError(f"{exit_code=}")

    logger.info(f"{exit_code=}")
    return exit_code


@task(tags=["dask-resource:pix=1", "dask-resource:gui=1"])
def run_cmd_w_gui(cmd: list[str]) -> int:
    """run command string in terminal

    Args:
        cmd (list[str]): command string list for execution

    Raises:
        RuntimeError: error if exit code != 0

    Returns:
        int: exit code
    """
    logger = prefect.context.get("logger")
    logger.info(f"run {cmd=}")

    p = subprocess.run(cmd, capture_output=True, encoding="cp866")

    logger.info(p.stdout)
    logger.error(p.stderr)
    print(p.stdout)
    exit_code = p.returncode

    if exit_code != 0:
        raise RuntimeError(f"{exit_code=}")

    logger.info(f"{exit_code=}")
    return exit_code


def get_pix_robot_path() -> str:
    """get path to robot.exe from registry

    Returns:
        str: full path to robot.exe
    """
    import winreg

    key_path = r"\Studio.Document\shell\open\command"
    registry = winreg.ConnectRegistry(None, winreg.HKEY_CLASSES_ROOT)

    k = winreg.OpenKey(registry, key_path)
    default_key = winreg.QueryValueEx(k, "")

    pix_studio_path = Path(re.findall('"([^"]+)"', default_key[0])[0])
    pix_robot_path = pix_studio_path.with_stem("Robot")

    return str(pix_robot_path.absolute())


@task
def combine_cmd_pix(
    script_path: str,
    script_parameters: Optional[Union[str, dict[str, Any]]] = None,
    robot_path: Optional[str] = None,
) -> list[str]:
    """Generate command string to start pix script in terminal.

    Args:
        script_path (str): path to pix-script
        script_parameters (Optional[Union[str, dict[str, Any]]], optional): script parameters in python-dict format or json-like string with single quotes. Defaults to None.
        robot_path (Optional[str], optional): path to robot executor (robot.exe). If "None" - search path in registry. Defaults to None.

    Returns:
        list[str]: command string list to start pix script in terminal
    """

    logger = prefect.context.get("logger")
    cmd = []

    if robot_path is None:
        try:
            robot_path = get_pix_robot_path()
        except FileNotFoundError:
            logger.error("Robot.exe not found. Is Robot PIX installed?")
            raise

    robot_path = Path(robot_path)
    cmd.append(str(robot_path.absolute()))
    cmd.extend(["-f", script_path])

    if script_parameters is not None:
        if type(script_parameters) is str:
            cmd.append(f'-p="{script_parameters}"')
        elif type(script_parameters) is dict:
            cmd.append("-p=" + json.dumps(script_parameters))
    generated_cmd = " ".join(cmd)
    logger.info(f"{generated_cmd=}")
    return cmd


@task
def bool_param(param):
    res = str(param).lower() in ("yes", "y", "true", "1")
    return res


work_dir = Path().absolute()
module_path = Path(__file__)
relative_path = module_path.relative_to(work_dir)

storage = Git(
    repo=Secret("GIT_REPO").get(),
    flow_path=relative_path.as_posix(),
    repo_host=Secret("GIT_SERVER_HOST").get(),
    branch_name=Secret("GIT_BRANCH").get(),
)

executor = DaskExecutor(address=Secret("DASK_SCHEDULER_ADDRESS").get())

with Flow("run_pix", executor=executor, storage=storage) as flow_runner_pix:
    script_path = Parameter("script_path", required=True)
    script_parameters = Parameter("script_parameters", required=False)
    robot_path = Parameter("robot_path", required=False)
    need_gui = Parameter("need_gui", default=True)

    cmd = combine_cmd_pix(script_path, script_parameters, robot_path)

    need_gui_cond = bool_param(need_gui)
    with case(need_gui_cond, True):
        result = run_cmd_w_gui(cmd)
    with case(need_gui_cond, False):
        result = run_cmd(cmd)


def test():
    parameters = {
        "script_path": r"task.pix",
        "script_parameters": {"time": "120"},
        "need_gui": False,
    }

    flow_runner_pix.run(
        parameters=parameters,
        idempotency_key="one_task_per_day",
    )


def main():
    flow_runner_pix.register(
        project_name="pix_robots",
        labels=["prefect"],
        add_default_labels=False,
    )


if __name__ == "__main__":
    main()
