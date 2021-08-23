import unittest
import logging
from typing import Any
from pyspark.sql.session import SparkSession
from dq_whistler.constraints.number_type import *
from tests.dq_whistler.resources.configuration import number_constraints

log= logging.getLogger( "SomeTest.testSomething" )


class NumberConstraintTests(unittest.TestCase):
	"""
		Test suite for number type column constraints
	"""
	spark_session: SparkSession
	column_data: DataFrame
	column_name: str = "number_col"
	constriants: Dict[str, Any]

	def setUp(self):
		"""
		"""
		pass

	def tearDown(self):
		"""
		"""
		pass

	def test_equal_pass(self):
		self.column_data = self.spark_session.createDataFrame([(1, ), (1, )]).toDF(self.column_name)
		constraint = number_constraints["eq"]
		output = Equal(constraint=constraint, column_name=self.column_name).execute_check(self.column_data)
		self.assertConstraintPass(output, constraint=constraint)

	def test_equal_fail(self):
		pass

	def test_not_equal(self):
		pass

	def test_less_than(self):
		pass

	def test_greater_than(self):
		pass

	def test_less_than_equal_to(self):
		pass

	def test_greater_than_equal_to(self):
		pass

	def test_between(self):
		pass

	def test_not_between(self):
		pass

	def test_is_in(self):
		pass

	def test_not_in(self):
		pass

	def assertConstraintPass(self, test_result: Dict[str, Any], constraint: Dict[str, Any]) -> None:
		return self.assertEqual(
			test_result, {
				**constraint,
				"constraint_status": "success",
				"invalid_count": 0,
				"invalid_values": []
			}
		)
