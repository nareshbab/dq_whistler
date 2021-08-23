from abc import ABC, abstractmethod
from typing import Dict, List
from pyspark.sql.dataframe import DataFrame
import pyspark.sql. functions as F
import json


class Constraint(ABC):
    def __init__(self, constraint: Dict[str, str], column_name: str):
        self._name = constraint.get("name")
        self._values = constraint.get("values")
        self._column_name = column_name
        self._constraint = constraint

    def constraint_name(self):
        return self._name

    def get_column_name(self):
        return self._column_name

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    @abstractmethod
    def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def get_sample_invalid_values(self, data_frame: DataFrame) -> List:
        """

        Args:
            data_frame:

        Returns:

        """
        sample_invalid_values = [json.loads(row)[self._column_name] for row in data_frame.toJSON().take(10)]
        return sample_invalid_values

    def execute_check(self, data_frame: DataFrame) -> Dict[str, str]:
        """

        Returns:

        """
        unmatched_df = self.get_failure_count(data_frame)
        unmatched_count = unmatched_df.count()
        sample_invalid_values = self.get_sample_invalid_values(unmatched_df)
        return {
            **self._constraint,
            "constraint_status": "failed" if unmatched_count > 0 else "success",
            "invalid_count": unmatched_count,
            "invalid_values": sample_invalid_values
        }
