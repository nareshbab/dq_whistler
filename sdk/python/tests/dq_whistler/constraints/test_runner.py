import unittest

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType, DoubleType, StringType
from tests.dq_whistler.constraints.test_number_constraints import NumberConstraintTests
from tests.dq_whistler.constraints.test_string_constraints import StringConstraintTests


def get_spark_session():
	spark_session = (
		SparkSession.builder
			.master("local[*]")
			.appName("dq_whistler_test_cases")
			.getOrCreate()
	)
	column_data = spark_session.createDataFrame(
		[
			('abc', '2020-11-03T06:24:42.000Z', 1),
			('abcd', '2020-11-11T10:13:42.000Z', 4),
			('abcde', '2020-11-12T11:08:42.000Z', 5),
			('xyz', '2020-11-17T11:08:42.000Z', 3)
		], ['string_col', 'time_col', 'number_col'],
	) \
	.withColumn("time_col", col("time_col").cast(TimestampType())) \
	.withColumn("string_col", col("string_col").cast(StringType())) \
	.withColumn("number_col", col("number_col").cast(DoubleType()))

	return spark_session


def run_tests():
	"""
		Runs test cases in specific classes
	Returns:

	"""
	spark_session = get_spark_session()
	test_classes = [NumberConstraintTests, StringConstraintTests]

	loader = unittest.TestLoader()
	test_suites = []
	for test_class in test_classes:
		test_class.spark_session = spark_session
		suite = loader.loadTestsFromTestCase(test_class)
		test_suites.append(suite)

	big_suite = unittest.TestSuite(test_suites)
	runner = unittest.TextTestRunner()
	runner.run(big_suite)


if __name__ == "__main__":
	run_tests()
