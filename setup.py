from setuptools import setup

setup(
    entry_points = {
        "kedro.project_commands": [
            "kedro_monitor = kedro_monitor.frameworks.cli.cli:commands"
        ],
        "kedro.hooks": [
            "kedro_monitor_mlflow = kedro_monitor.mlflow.hooks:mlflow_hook"
        ]
    }
)
