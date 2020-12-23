from typing import Dict, Any, Callable
import kedro
import mlflow

from kedro.framework.session import get_current_session

from pathlib import Path


class MlflowRecorder(object):
    def __init__(self):
        self.params = {}
        self.metrics = {}
        self.models = {}
        self.artifacts = {}

        self.session = get_current_session()
        self.context = self.session.load_context()
        self.cat_conf = self.context.config_loader.get('catalog*')

    def set_data(self, data: Dict[str, Any]):
        """
        expected dict structure
        Set the float value at params&metrics
        Set the catalog name as value at models&artifacts
        data = {
            "params": {"lr": 0.01, "n_class": 2},
            "metrics": {"f1": 0.77, "auc": 0.79},
            "models": {"linearregression": "lrmodel"},
            "artifacts": {"confusion_matrix": "plot_confmat"},
        }
        """
        self.params = data["params"]
        self.metrics = data["metrics"]
        for k, v in data["models"].items():
            val = self.cat_conf[v]["filepath"]
            self.models[k] = val
        for k, v in data["artifacts"].items():
            val = self.cat_conf[v]["filepath"]
            self.artifacts[k] = val


NodeFunc = Callable[[Any], Any]


def record_mlflow(f: NodeFunc):
    def decorator(*args, **kwargs):
        dictrec, *result = f(*args, **kwargs)
        mlrec = MlflowRecorder()
        mlrec.set_data(dictrec)

        if len(mlrec.params) != 0:
            mlflow.log_params(mlrec.params)
        if len(mlrec.metrics) != 0:
            mlflow.log_metrics(mlrec.metrics)
        for i in mlrec.artifacts.values():
            mlflow.log_artifact(i)
        return result
    return decorator


def record_sklearn_mlflow(f: NodeFunc):
    def decorator(*args, **kwargs):
        mlflow.sklearn.autolog()
        return f(*args, **kwargs)
    return decorator


def record_pytorch_mlflow(f: NodeFunc):
    def decorator(*args, **kwargs):
        mlflow.pytorch.autolog()
        return f(*args, **kwargs)
    return decorator
