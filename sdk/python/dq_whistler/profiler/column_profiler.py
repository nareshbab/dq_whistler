from abc import ABC, abstractmethod
from typing import Dict, Any, List
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, DoubleType, IntegerType
from dq_whistler.constraints.constraint import Constraint
import dq_whistler.constraints.number_type as number_constraints
import dq_whistler.constraints.string_type as string_constraints
import json


class ColumnProfiler(ABC):
    """
    Base class for Column profiler
    Args:

    Returns:

    Raises:

    Examples:
    """
    _column: DataFrame
    _config: Dict[str, Any]
    _constraints: List[Constraint]

    def __init__(self, column_data: DataFrame, config: Dict[str, Any]):
        """
        Initializes the column profiler object
        """
        self._column_data = column_data
        self._config = config
        self._column_name = config.get("name")
        self._data_type = config.get("datatype")
        self._constraints = []

    def prepare_df_for_constraints(self) -> None:
        if self._data_type == "string":
            self._column_data.withColumn(self._column_name, f.col(self._column_name).cast(StringType()))
        elif self._data_type == "number":
            self._column_data.withColumn(self._column_name, f.col(self._column_name).cast(DoubleType()))
        elif self._data_type == "integer":
            self._column_data.withColumn(self._column_name, f.col(self._column_name).cast(IntegerType()))
        else:
            raise NotImplementedError

    def add_constraint(self, constraint: Constraint):
        existing = filter(
            lambda c:
            c.constraint_name() == constraint.constraint_name()
            and c.get_column_name() == constraint.get_column_name(), self._constraints)
        if list(existing):
            raise ValueError(f"A similar constraint for the column {constraint.get_column_name()} already exists.")
        self._constraints.append(constraint)

    def get_constraints_config(self) -> List[Constraint]:
        for constraint in self._config.get("constraints"):
            self.add_constraint(constraint=constraint)
        return self._constraints

    def get_column_info(self) -> str:
        """
        Returns:
            The name of the column
        """
        return self._column_data.schema.json()

    def get_column_config(self) -> Dict[str, Any]:
        """
        Returns:
            Data quality resources for a particular column
        """
        return self._config

    def get_null_count(self) -> int:
        """
        Returns:
            Total null count of the column
        """
        col_name = self._column_name
        return self._column_data.select(
            f.count(
                f.when(
                    f.col(col_name).contains("None") |
                    f.col(col_name).contains("NULL") |
                    (f.col(col_name) == "") |
                    f.col(col_name).isNull() |
                    f.isnan(col_name), col_name
                )
            ).alias("null_count")
        ).first()[0]

    def get_unique_count(self) -> int:
        """
        Returns:
            The count of unique rows i.e non duplicating
        """
        return self._column_data.distinct().count()

    def get_total_count(self) -> int:
        """
        Returns:
            The total count of rows
        """
        return self._column_data.count()

    def get_quality_score(self) -> float:
        """
        Returns:
            The overall score of the column
        """
        return 0.0

    def get_topn(self) -> Dict[str, Any]:
        """
        Returns:
            The topn values as a sample along with the count of values
            {"value1": count1, "value2": count2}, {"a": 1, "b":2}
        """
        #TODO:: Remove null values before getting top n values
        col_name = self._column_name
        top_values = dict()
        top_values_rows = self._column_data \
            .groupby(col_name) \
            .count() \
            .sort(f.desc("count")) \
            .toJSON() \
            .take(10)
        [top_values.update({json.loads(row)[col_name]: json.loads(row)["count"]}) for row in top_values_rows]
        return top_values

    def get_custom_constraint_check(self):
        """
        Returns:
            The count of rows matching a user's custom constraint
            The sample values failing constraint check
            constraint result to be true or false depending whether there are rows matching that constraint
        """
        constraints_output = []
        for constraint in self._constraints:
            output = constraint.execute_check(self._column_data)
            constraints_output.append(output)
        return constraints_output

    @abstractmethod
    def run(self) -> Dict[str, Any]:
        """
        Returns:
            The final stats of the column containing null checks, total count,
            regex matches, invalid rows, quality score etc.
        """
        pass
