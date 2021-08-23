import json

from pyspark.sql import DataFrame
from typing import Dict, List, Any
import pyspark.sql.functions as F
from dq_whistler.profiler.string_profiler import StringProfiler
from dq_whistler.profiler.number_profiler import NumberProfiler

class DataQualityAnalyzer:
	"""

	"""
	_data: DataFrame
	_config: List[Dict[str, str]]

	def __init__(self, data: DataFrame, config: List[Dict[str, str]]):
		self._data = data
		self._config = config

	def analyze(self) -> str:
		final_checks: List[Dict[str, Any]] = []
		for column_config in self._config:
			# TODO:: checks for key existence
			column_name = column_config.get("name")
			column_data_type = column_config.get("datatype")
			column_data = self._data.select(F.col(column_name))
			if column_data_type == "string":
				profiler = StringProfiler(column_data, column_config)
			elif column_data_type == "number":
				profiler = NumberProfiler(column_data, column_config)
			else:
				raise NotImplementedError
			output = profiler.run()
			final_checks.append({
				"col_name": column_name,
				**output
			})
		return json.dumps(final_checks)