"""Debug tests for case-insensitive column resolution."""
import pytest
from sparkless.sql import SparkSession, functions as F
from sparkless.spark_types import StructType, StructField, StringType, IntegerType


class TestDebugRemaining:
    @pytest.fixture
    def spark(self):
        return SparkSession("TestApp")

    def test_nested_struct_correct_case(self, spark):
        """Check if nested struct access works with correct case."""
        data = [
            {"Person": {"Name": "Alice", "Age": 25}},
        ]
        schema = StructType([
            StructField("Person", StructType([
                StructField("Name", StringType()),
                StructField("Age", IntegerType()),
            ])),
        ])
        df = spark.createDataFrame(data, schema=schema)

        # Correct case
        result = df.select("Person.Name").collect()
        assert result[0]["Person.Name"] == "Alice", \
            f"correct case: {result[0]['Person.Name']}"

    def test_nested_struct_wrong_case(self, spark):
        """Check if nested struct access works with wrong case."""
        data = [
            {"Person": {"Name": "Alice", "Age": 25}},
        ]
        schema = StructType([
            StructField("Person", StructType([
                StructField("Name", StringType()),
                StructField("Age", IntegerType()),
            ])),
        ])
        df = spark.createDataFrame(data, schema=schema)

        # Wrong case
        result = df.select("Person.name").collect()
        assert result[0]["Person.name"] == "Alice", \
            f"wrong case: {result[0]['Person.name']}"

    def test_distinct_after_select_correct_case(self, spark):
        """Check if distinct after select works with correct case."""
        data = [
            {"Name": "Alice", "Age": 25, "Salary": 5000, "Dept": "IT"},
            {"Name": "Bob", "Age": 30, "Salary": 6000, "Dept": "HR"},
        ]
        df = spark.createDataFrame(data)
        df_with_dupes = df.union(df)

        result_correct = df_with_dupes.select("Name").distinct().collect()
        assert len(result_correct) == 2, f"correct case: {len(result_correct)}"

    def test_distinct_after_select_wrong_case(self, spark):
        """Check if distinct after select works with wrong case."""
        data = [
            {"Name": "Alice", "Age": 25, "Salary": 5000, "Dept": "IT"},
            {"Name": "Bob", "Age": 30, "Salary": 6000, "Dept": "HR"},
        ]
        df = spark.createDataFrame(data)
        df_with_dupes = df.union(df)

        result_wrong = df_with_dupes.select("name").distinct().collect()
        assert len(result_wrong) == 2, f"wrong case: {len(result_wrong)}"
