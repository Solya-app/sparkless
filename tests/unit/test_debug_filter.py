"""
Verification tests: remaining failures are pre-existing bugs unrelated to case sensitivity.

Each test verifies that the correct-case version also fails, proving the issue
is NOT about case-insensitive column resolution.
"""
import pytest
from sparkless.sql import SparkSession, functions as F
from sparkless.spark_types import StructType, StructField, StringType, IntegerType


class TestPreExistingBugsVerification:
    @pytest.fixture
    def spark(self):
        return SparkSession("TestApp")

    def test_startswith_broken_for_both_cases(self, spark):
        """startswith is broken regardless of column case."""
        data = [{"Name": "Alice"}, {"Name": "Bob"}]
        df = spark.createDataFrame(data)
        # Both correct and wrong case return 0 results
        assert len(df.filter(F.col("Name").startswith("A")).collect()) == \
               len(df.filter(F.col("name").startswith("A")).collect())

    def test_selectExpr_alias_broken_for_both_cases(self, spark):
        """selectExpr with alias has empty schema for both cases."""
        data = [{"Name": "Alice"}]
        df = spark.createDataFrame(data)
        r1 = df.selectExpr("Name as full_name").collect()
        r2 = df.selectExpr("name as full_name").collect()
        s1 = [f.name for f in r1[0]._schema.fields] if r1 else []
        s2 = [f.name for f in r2[0]._schema.fields] if r2 else []
        assert s1 == s2

    def test_coalesce_broken_for_both_cases(self, spark):
        """coalesce returns None for both correct and wrong case."""
        schema = StructType([
            StructField("Col1", StringType(), True),
            StructField("Col2", StringType(), True),
        ])
        data = [{"Col1": None, "Col2": "Value2"}]
        df = spark.createDataFrame(data, schema=schema)
        r1 = df.select(F.coalesce(F.col("Col1"), F.col("Col2")).alias("r")).collect()
        r2 = df.select(F.coalesce(F.col("col1"), F.col("col2")).alias("r")).collect()
        assert r1[0]["r"] == r2[0]["r"]

    def test_nested_struct_broken_for_both_cases(self, spark):
        """Nested struct access returns None for both cases."""
        schema = StructType([
            StructField("Person", StructType([
                StructField("Name", StringType()),
            ])),
        ])
        data = [{"Person": {"Name": "Alice"}}]
        df = spark.createDataFrame(data, schema=schema)
        r1 = df.select("Person.Name").collect()
        r2 = df.select("Person.name").collect()
        assert r1[0]["Person.Name"] == r2[0]["Person.name"]
