from dq_whistler.constraints.constraint import Constraint
from typing import Dict
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


class Equal(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"eq",
			"values": "abc"
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
			F.col(self._column_name) != self._values
		)


class NotEqual(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"nt_eq",
			"values": "abc"
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
			F.col(self._column_name) == self._values
		)


class Contains(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"contains",
			"values": "abc"
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
			~F.col(self._column_name).contains(self._values)
		)


class NotContains(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"not_contains",
			"values": "abc"
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
			F.col(self._column_name).contains(self._values)
		)


class StartsWith(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"starts_with",
			"values": "abc"
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
			~F.col(self._column_name).startswith(self._values)
		)


class NotStartsWith(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"not_starts_with",
			"values": "abc"
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
			F.col(self._column_name).startswith(self._values)
		)


class EndsWith(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"ends_with",
			"values": "abc"
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
			~F.col(self._column_name).endswith(self._values)
		)


class NotEndsWith(Constraint):
	"""
	Args:
		constraint (Dict[str, str]):
		{
			"name":"custom user name",
			"type":"constraint",
			"sub_type":"not_ends_with",
			"values": "abc"
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
			F.col(self._column_name).endswith(self._values)
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
			~F.col(self._column_name).isin(*self._values)
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
			F.col(self._column_name).isin(self._values)
		)


class Regex(Constraint):
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
            ~F.col(self._column_name)
            .rlike(self._values)
        )
