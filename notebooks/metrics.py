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
import heapq
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


class MetricCalculator:
    def __init__(self, preds_labels_df: pd.DataFrame, top_k: int = 5):
        self.preds, self.labels, self.preds_top, self.labels_top = [], [], [], []
        self.top_k = top_k
        self.extract_labels_preds(preds_labels_df)
        self.extract_labels_preds_top_k(preds_labels_df, top_k)
        self.thresholds = sorted(list(set(self.preds)))
        self.class_pred_metrics = self.calculate_class_pred_metrics()
        self.metrics = pd.concat(
            [
                self.maximize_class_metrics(),
                self.calculate_pred_metrics(),
                self.get_top_k_metrics(),
            ]
        )

    def extract_labels_preds_top_k(self, preds_labels_df: pd.DataFrame, k: int = 5):
        logger.info(f"Extracting labels and preds for top {k} moments from DataFrame")
        if (
            "labels" not in preds_labels_df.index
            or "preds" not in preds_labels_df.index
        ):
            raise ValueError("The DataFrame must have labels and preds in index")
        for vid_id in preds_labels_df.columns:
            preds = heapq.nlargest(k, preds_labels_df[vid_id].loc["preds"])
            self.preds_top += preds
            labels = [
                preds_labels_df[vid_id].loc["labels"][i]
                for i in [preds_labels_df[vid_id].loc["preds"].index(p) for p in preds]
            ]
            self.labels_top += labels

    def extract_labels_preds(self, preds_labels_df: pd.DataFrame):
        logger.info("Extracting labels and preds from DataFrame")
        if (
            "labels" not in preds_labels_df.index
            or "preds" not in preds_labels_df.index
        ):
            raise ValueError("The DataFrame must have labels and preds in index")
        self.preds = list(chain.from_iterable(preds_labels_df.loc["preds"].tolist()))
        self.labels = list(chain.from_iterable(preds_labels_df.loc["labels"].tolist()))

    def calculate_class_pred_metrics(self, top_k_calc: bool = False) -> pd.DataFrame:
        logger.info("Calculating class metrics for each threshold")
        thresholds = self.thresholds if not top_k_calc else [min(self.preds_top)]
        return pd.DataFrame(
            {
                threshold: self.calculate_threshold_metrics(threshold, top_k_calc)
                for threshold in thresholds
            }
        ).T

    def get_metric_value(self, threshold, metric_name) -> float:
        return self.class_pred_metrics.loc[threshold, metric_name]

    def calculate_pred_metrics(self, top_k_calc: bool = False) -> pd.DataFrame:
        logger.info("Calculating metrics for each threshold")
        # roc_auc = None if top_k_calc else roc_auc_score(self.labels, self.preds)
        try:
            roc_auc = roc_auc_score(
                self.labels if not top_k_calc else self.labels_top,
                self.preds if not top_k_calc else self.preds_top,
            )
        except:
            roc_auc = None
        try:
            average_precision = average_precision_score(
                self.labels if not top_k_calc else self.labels_top,
                self.preds if not top_k_calc else self.preds_top,
            )
        except:
            average_precision = None
        return pd.DataFrame(
            {
                "index": [
                    f"roc_auc{'_top' + str(self.top_k) if top_k_calc else ''}",
                    f"average_precision{'_top' + str(self.top_k) if top_k_calc else ''}",
                ],
                "threshold": [None] * 2,
                "value": [roc_auc, average_precision],
            }
        )

    def get_top_k_metrics(self) -> pd.DataFrame:
        logger.info(f"Calculating metrics for top {self.top_k} moments")
        class_pred_metrics = self.calculate_class_pred_metrics(
            top_k_calc=True
        ).T.reset_index()
        threshold = class_pred_metrics.columns[1]
        return pd.concat(
            [
                class_pred_metrics.rename(columns={threshold: "value"}).assign(
                    threshold=threshold
                )[["index", "threshold", "value"]],
                self.calculate_pred_metrics(top_k_calc=True),
            ]
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

    def calculate_threshold_metrics(
        self, threshold: float, top_k_calc: bool
    ) -> Dict[str, float]:
        preds = self.preds_top if top_k_calc else self.preds
        labels = self.labels_top if top_k_calc else self.labels
        class_preds = [1 if pred >= threshold else 0 for pred in preds]
        return {
            f"f1{'_top' + str(self.top_k) if top_k_calc else ''}": f1_score(
                labels, class_preds
            ),
            f"accuracy{'_top' + str(self.top_k) if top_k_calc else ''}": accuracy_score(
                labels, class_preds
            ),
            f"precision{'_top' + str(self.top_k) if top_k_calc else ''}": precision_score(
                labels, class_preds
            ),
            f"recall{'_top' + str(self.top_k) if top_k_calc else ''}": recall_score(
                labels, class_preds
            ),
            f"kappa{'_top' + str(self.top_k) if top_k_calc else ''}": cohen_kappa_score(
                labels, class_preds
            ),
        }
