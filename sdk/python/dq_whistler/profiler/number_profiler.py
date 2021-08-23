from dq_whistler.profiler.column_profiler import ColumnProfiler
from dq_whistler.constraints.number_type import *
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Dict, Any
import json


class NumberProfiler(ColumnProfiler):
	"""

	"""

	def __init__(self, column_data: DataFrame, config: Dict[str, str]):
		super(NumberProfiler, self).__init__(column_data, config)

	def get_min_value(self):
		"""

		Returns:

		"""
		min_value = json.loads(self._column_data.select(
			f.min(
				f.col(self._column_name)
					.cast("double")
			).alias("min")
		).toJSON().take(1)[0])["min"]
		return min_value

	def get_max_value(self):
		"""

		Returns:

		"""
		max_value = json.loads(self._column_data.select(
			f.max(
				f.col(self._column_name)
					.cast("double")
			).alias("max")
		).toJSON().take(1)[0])["max"]
		return max_value

	def get_mean_value(self):
		"""

		Returns:

		"""
		mean_value = json.loads(self._column_data.select(
			f.mean(
				f.col(self._column_name)
					.cast("double")
			).alias("mean")
		).toJSON().take(1)[0])["mean"]
		return mean_value

	def get_stddev_value(self):
		"""

		Returns:

		"""
		stddev_value = json.loads(self._column_data.select(
			f.stddev(
				f.col(self._column_name)
					.cast("double")
			).alias("stddev")
		).toJSON().take(1)[0])["stddev"]
		return stddev_value

	def run(self) -> Dict[str, Any]:
		"""

		Returns:

		"""
		column_name = self._column_name
		for constraint in self._config.get("constraints"):
			name = constraint.get("name")
			if name == "eq":
				self.add_constraint(
					Equal(constraint=constraint, column_name=column_name)
				)
			elif name == "not_eq":
				self.add_constraint(
					NotEqual(constraint=constraint, column_name=column_name)
				)
			elif name == "lt":
				self.add_constraint(
					LessThan(constraint=constraint, column_name=column_name)
				)
			elif name == "gt":
				self.add_constraint(
					GreaterThan(constraint=constraint, column_name=column_name)
				)
			elif name == "lt_eq":
				self.add_constraint(
					LessThanEqualTo(constraint=constraint, column_name=column_name)
				)
			elif name == "gt_eq":
				self.add_constraint(
					GreaterThanEqualTo(constraint=constraint, column_name=column_name)
				)
			elif name == "between":
				self.add_constraint(
					Between(constraint=constraint, column_name=column_name)
				)
			elif name == "not_between":
				self.add_constraint(
					NotBetween(constraint=constraint, column_name=column_name)
				)
			elif name == "is_in":
				self.add_constraint(
					IsIn(constraint=constraint, column_name=column_name)
				)
			elif name == "not_in":
				self.add_constraint(
					NotIn(constraint=constraint, column_name=column_name)
				)
			else:
				raise NotImplementedError
		# Preparing data frame for constraints execution
		self.prepare_df_for_constraints()
		# Get final output of constraints
		output = self.get_custom_constraint_check()
		return {
			"total_count": self.get_total_count(),
			"null_count": self.get_null_count(),
			"unique_count": self.get_unique_count(),
			"topn_values": self.get_topn(),
			"min": self.get_min_value(),
			"max": self.get_max_value(),
			"mean": self.get_mean_value(),
			"stddev": self.get_stddev_value(),
			"quality_score": self.get_quality_score(),
			"constraints": output
		}
