from itertools import chain
import pandas as pd
from typing import Dict
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    f1_score,
    accuracy_score,
    precision_score,
    cohen_kappa_score,
    recall_score,
)

from loguru import logger


class MetricCalculator:
    def __init__(self, preds_labels_df: pd.DataFrame):
        self.preds = self.labels = []
        self.extract_labels_preds(preds_labels_df)
        self.thresholds = sorted(list(set(self.preds)))
        self.class_pred_metrics = self.calculate_class_pred_metrics()
        self.metrics = pd.concat(
            [self.maximize_class_metrics(), self.calculate_pred_metrics()]
        )

    def extract_labels_preds(self, preds_labels_df: pd.DataFrame):
        logger.info("Extracting labels and preds from DataFrame")
        if (
            "labels" not in preds_labels_df.index
            or "preds" not in preds_labels_df.index
        ):
            raise ValueError("The DataFrame must have labels and preds in index")
        self.preds = list(chain.from_iterable(preds_labels_df.loc["preds"].tolist()))
        self.labels = list(chain.from_iterable(preds_labels_df.loc["labels"].tolist()))

    def calculate_class_pred_metrics(self) -> pd.DataFrame:
        logger.info("Calculating class metrics for each threshold")
        return pd.DataFrame(
            {
                threshold: self.calculate_threshold_metrics(threshold)
                for threshold in self.thresholds
            }
        ).T

    def get_metric_value(self, threshold, metric_name) -> float:
        return self.class_pred_metrics.loc[threshold, metric_name]

    def calculate_pred_metrics(self) -> pd.DataFrame:
        logger.info("Calculating metrics for each threshold")
        return pd.DataFrame(
            {
                "index": ["roc_auc", "average_precision"],
                "threshold": [None] * 2,
                "value": [
                    roc_auc_score(self.labels, self.preds),
                    average_precision_score(self.labels, self.preds),
                ],
            }
        )

    def maximize_class_metrics(self) -> pd.DataFrame:
        logger.info("Maximizing class metrics")
        maximized_metric_indices = self.class_pred_metrics.idxmax().reset_index(
            name="threshold"
        )
        maximized_metric_indices["value"] = maximized_metric_indices.apply(
            lambda row: self.get_metric_value(row["threshold"], row["index"]), axis=1
        )
        return maximized_metric_indices

    def calculate_threshold_metrics(self, threshold: float) -> Dict[str, float]:
        preds = [1 if pred >= threshold else 0 for pred in self.preds]
        return {
            "f1": f1_score(self.labels, preds),
            "accuracy": accuracy_score(self.labels, preds),
            "precision": precision_score(self.labels, preds),
            "recall": recall_score(self.labels, preds),
            "kappa": cohen_kappa_score(self.labels, preds),
        }
