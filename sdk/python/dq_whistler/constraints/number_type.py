from dq_whistler.constraints.constraint import Constraint
from typing import Dict
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


class Equal(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"eq",
			"values": 5
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name) != self._values
		)


class NotEqual(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"nt_eq",
			"values": 5
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name) == self._values
		)


class LessThan(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"lt",
			"values": 5
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name) >= self._values
		)


class GreaterThan(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"gt",
			"values": 5
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name) <= self._values
		)


class LessThanEqualTo(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"lt_eq",
			"values": 5
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name) > self._values
		)


class GreaterThanEqualTo(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"gt_eq",
			"values": 5
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name) < self._values
		)

class Between(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"between",
			"values": [3, 4]
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			~f.col(self._column_name).between(*self._values)
		)


class NotBetween(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"not_between",
			"values": [3, 5]
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name).between(*self._values)
		)


class IsIn(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"is_in",
			"values": [1, 2, 3]
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			~f.col(self._column_name).isin(*self._values)
		)


class NotIn(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"not_in",
			"values": [1, 2, 3]
		}
	"""

	def __init__(self, constraint: Dict[str, str], column_name: str):
		super().__init__(constraint, column_name)

	def get_failure_count(self, data_frame: DataFrame) -> DataFrame:
		"""

		Args:
			data_frame:

		Returns:

		"""
		return data_frame.filter(
			f.col(self._column_name).isin(self._values)
		)
	