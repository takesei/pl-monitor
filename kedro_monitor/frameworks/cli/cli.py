from pathlib import Path
import subprocess
from textwrap import indent

import click
from cookiecutter.main import cookiecutter
import kedro
from kedro.framework.cli.utils import (
    KedroCliError,
    _clean_pycache,
    command_with_verbosity,
)
from kedro.framework.context import load_context
from kedro.framework.project.settings import _get_project_settings
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata

import kedro_monitor
from kedro_monitor.frameworks.context.context import get_monitor_config


@click.group(name="Monitor")
def commands():
    """Commands for working with monitor tools."""
    pass  # pragma: no cover


@commands.command(name="monitor", cls=click.Group)
def monitor():
    """Monitor: Supported tool"""
    pass


@monitor.command(name="mlflow", cls=click.Group)
def mlflow_commands():
    """Mlflow: Mlflow tool"""


@command_with_verbosity(mlflow_commands, "new")
@click.option(
    "--experiment-name",
    "-x",
    required=False,
    default="<package_name>",
    help="Default Experiment name"
)
@click.pass_obj
def create_mlflow(
    meta: ProjectMetadata, experiment_name: str, **kwargs
):
    """
    Set up new mlflow environment
    """
    if experiment_name == "<package_name>":
        experiment_name = meta.package_name
    conf_root = _get_project_settings(meta.package_name, "CONF_ROOT", "conf")
    project_conf_path = meta.project_path / conf_root

    result_path = _create_new_mlflow(
        experiment_name,
        project_conf_path / "base",
        "mlflow"
    )

    click.secho(
        f"\nMlflow env `{experiment_name}` was successfully created.\nCheck {result_path}",
        fg="green"
    )


def _create_new_mlflow(name: str, output_dir: Path, pkg_name: str) -> Path:
    template_path = Path(kedro_monitor.__file__).parent / "templates" / pkg_name
    cookie_context = {
        "dir_name": pkg_name,
        "experiment_name": name,
        "kedro_version": kedro.__version__,
        "kedro_monitor_version": kedro_monitor.__version__
    }
    click.echo(f"Creating the new mlflow environment `{name}`: ", nl=False)
    try:
        result_path = cookiecutter(
            str(template_path),
            output_dir=str(output_dir),
            no_input=True,
            extra_context=cookie_context,
        )
    except Exception as exc:
        click.secho("FAILED", fg="red")
        cls = exc.__class__
        raise KedroCliError(
            f"{cls.__module__}.{cls.__qualname__}: {exc}"
        ) from exc
    click.secho("OK", fg="green")
    result_path = Path(result_path)
    message = indent(f"Location: `{result_path.resolve()}`", " " * 2)
    click.secho(message, bold=True)

    _clean_pycache(result_path)

    return result_path


@mlflow_commands.command()
@click.option(
    "--env",
    "-e",
    required=False,
    default="local",
    help="The environment within conf folder we want to retrieve.",
)
@click.option(
    "--conf",
    "-c",
    required=False,
    default="",
    help="The environment within conf folder we want to retrieve.",
)
@click.pass_obj
def ui(meta: ProjectMetadata, env, conf):
    """Opens the mlflow user interface with the
    project-specific settings of mlflow.yml. This interface
    enables to browse and compares runs.
    """

    session = _create_session(meta.package_name, env=env)
    context = session.load_context()
    mlflow_conf = get_monitor_config(context, "mlflow")

    if conf == "":
        conf = mlflow_conf["mlflow_tracking_uri"]
    # call mlflow ui with specific options
    subprocess.call([
        "mlflow",
        "ui",
        "--backend-store-uri",
        conf
    ])


def _create_session(package_name: str, **kwargs):
    kwargs.setdefault("save_on_close", False)
    try:
        return KedroSession.create(package_name, **kwargs)
    except Exception as exc:
        raise KedroCliError(
            f"Unable to instantiate Kedro session.\nError: {exc}"
        ) from exc
