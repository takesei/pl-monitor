from kedro.framework.session import get_current_session
from kedro.pipeline import node
from kedro.pipeline.node import Node
import mlflow


def launch_mlflow_op(
    tracking_uri: str = None, experiment: str = None
) -> Node:
    return node(
        func=_launch_mlflow_op,
        tags=["mlflow", "launch"],
        inputs=[tracking_uri, experiment],
        outputs="params:mlflow_status"
    )


def _launch_mlflow_op(tracking_uri: str = None, experiment: str = None):
    session = get_current_session()
    context = session.load_context()
    conf_monitor = context.config_loader.get("mlflow*", "mlflow*/**")

    if tracking_uri is None:
        tracking_uri = conf_monitor["mlflow_tracking_uri"]
    if experiment is None:
        tracking_uri = conf_monitor["experiment"]["name"]

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)
    return True
