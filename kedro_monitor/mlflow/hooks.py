import mlflow

from kedro.framework.hooks import hook_impl
from kedro_monitor.mlflow.decorator import record_mlflow
from kedro_monitor.frameworks.context.context import get_monitor_config

from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.io import DataCatalog
from kedro.framework.context import load_context

from typing import Dict, Any


class MlflowHook:
    def __init__(self):
        self.run_params = None

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog):
        self.run_params = run_params

    @hook_impl
    def after_pipeline_run(
        self,
        run_params: Dict[str, Any],
        pipeline: Pipeline,
        catalog: DataCatalog,
    ) -> None:
        mlflow.end_run()

    @hook_impl
    def on_pipeline_error(
        self,
        error: Exception,
        run_params: Dict[str, Any],
        pipeline: Pipeline,
        catalog: DataCatalog
    ):
        while mlflow.active_run():
            mlflow.end_run()

    @hook_impl
    def before_node_run(self, node: Node):
        if "mlflow" in node.tags:
            if "launch" in node.tags:
                pass
            else:
                node.func = record_mlflow(node.func)

    @hook_impl
    def after_node_run(self, node: Node):
        if "mlflow" in node.tags and "launch" in node.tags:
            self.context = load_context(
                project_path=self.run_params["project_path"],
                env=self.run_params["env"],
                extra_params=self.run_params["extra_params"],
            )

            mlflow_conf = get_monitor_config(self.context, "mlflow")

            run_name = (
                mlflow_conf["run"]["name"]
                if mlflow_conf["run"]["name"] is not None
                else self.run_params["pipeline_name"]
            )
            mlflow.start_run(
                run_id=mlflow_conf["run"]["id"],
                run_name=run_name,
                nested=mlflow_conf["run"]["nested"],
            )
            mlflow.set_tags(self.run_params)


mlflow_hook = MlflowHook()
