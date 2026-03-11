"""Reproduction for robin-sparkless parity: count() with multiple count exprs causes duplicate column name.

Run with Sparkless (Robin): MOCK_SPARK_TEST_BACKEND=robin python -c "exec(open('tests/tools/robin_issues_repro/issue_count_duplicate_column.py').read())"
Run with PySpark: MOCK_SPARK_TEST_BACKEND=pyspark python -c "exec(open('tests/tools/robin_issues_repro/issue_count_duplicate_column.py').read())"
"""
from sparkless import SparkSession
from sparkless import functions as F

spark = SparkSession.builder.appName("count-dup").getOrCreate()
df = spark.createDataFrame([(1, "a", 10), (1, "b", 20), (2, "c", 30)], ["g", "x", "v"])
# Robin fails: duplicate column name 'count'; PySpark allows with aliases
result = df.groupBy("g").agg(F.count("*").alias("cnt1"), F.count("x").alias("cnt2"))
result.collect()
spark.stop()
