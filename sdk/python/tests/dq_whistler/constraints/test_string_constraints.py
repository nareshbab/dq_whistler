import unittest
from typing import Any
from pyspark.sql.session import SparkSession
from dq_whistler.constraints.string_type import *


class StringConstraintTests(unittest.TestCase):
	"""
		Test suite for number type column constraints
	"""
	config: Dict[str, Any]
	spark_session: SparkSession
	column_data: DataFrame

	def setUp(self):
		"""
		"""
		pass

	def tearDown(self):
		"""
		"""
		pass

	def test_equal(self):
		pass

	def test_not_equal(self):
		pass

	def test_contains(self):
		pass

	def test_not_contains(self):
		pass

	def test_starts_with(self):
		pass

	def test_not_starts_with(self):
		pass

	def test_ends_with(self):
		pass

	def test_not_ends_with(self):
		pass

	def test_is_in(self):
		pass

	def test_not_in(self):
		pass

	def test_regex(self):
		pass
