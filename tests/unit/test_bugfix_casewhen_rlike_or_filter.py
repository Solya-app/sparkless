"""Tests for bugfixes: CaseWhen in comparisons, rlike in filter, OR filter with isNull, percent_rank."""

import pytest

from sparkless.session import SparkSession
from sparkless.functions import F
from sparkless.sql import Window


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


class TestCaseWhenInComparisons:
    """Bug 1: CaseWhen objects must be resolved in comparisons."""

    def test_filter_col_le_when(self, spark):
        """df.filter(F.col('x') <= F.when(...).otherwise(...)) should work."""
        df = spark.createDataFrame(
            [
                {"x": 3, "y": 1},
                {"x": 8, "y": 1},
                {"x": 2, "y": -1},
                {"x": 6, "y": -1},
            ],
            "x INT, y INT",
        )
        result = df.filter(
            F.col("x") <= F.when(F.col("y") > 0, 10).otherwise(5)
        ).collect()
        # y>0 -> threshold=10, y<=0 -> threshold=5
        # x=3,y=1: 3<=10 True
        # x=8,y=1: 8<=10 True
        # x=2,y=-1: 2<=5 True
        # x=6,y=-1: 6<=5 False
        assert len(result) == 3
        assert sorted([r["x"] for r in result]) == [2, 3, 8]

    def test_filter_when_gt_col(self, spark):
        """F.when(...) on the left side of a comparison in filter."""
        df = spark.createDataFrame(
            [
                {"x": 3, "y": 1},
                {"x": 12, "y": 1},
            ],
            "x INT, y INT",
        )
        result = df.filter(
            F.when(F.col("y") > 0, 10).otherwise(5) >= F.col("x")
        ).collect()
        # threshold=10 for both; 10>=3 True, 10>=12 False
        assert len(result) == 1
        assert result[0]["x"] == 3

    def test_filter_col_eq_when(self, spark):
        """Equality comparison with CaseWhen."""
        df = spark.createDataFrame(
            [
                {"x": 10, "y": 1},
                {"x": 5, "y": -1},
                {"x": 99, "y": 0},
            ],
            "x INT, y INT",
        )
        result = df.filter(
            F.col("x") == F.when(F.col("y") > 0, 10).otherwise(5)
        ).collect()
        # y=1 -> 10==10 True; y=-1 -> 5==5 True; y=0 -> 99==5 False
        assert len(result) == 2


class TestRlikeInFilter:
    """Bug 2: rlike must work in filter() context."""

    def test_rlike_filter_basic(self, spark):
        df = spark.createDataFrame(
            [{"name": "Alice"}, {"name": "Bob"}, {"name": "Alicia"}],
            "name STRING",
        )
        result = df.filter(F.col("name").rlike("^A.*")).collect()
        assert len(result) == 2
        assert sorted([r["name"] for r in result]) == ["Alice", "Alicia"]

    def test_rlike_filter_no_match(self, spark):
        df = spark.createDataFrame(
            [{"name": "Alice"}, {"name": "Bob"}],
            "name STRING",
        )
        result = df.filter(F.col("name").rlike("^Z")).collect()
        assert len(result) == 0

    def test_rlike_filter_partial_match(self, spark):
        df = spark.createDataFrame(
            [{"code": "ABC-510-XYZ"}, {"code": "DEF-200-GHI"}],
            "code STRING",
        )
        result = df.filter(F.col("code").rlike(".*510.*")).collect()
        assert len(result) == 1
        assert result[0]["code"] == "ABC-510-XYZ"

    def test_rlike_filter_null_value(self, spark):
        df = spark.createDataFrame(
            [{"name": "Alice"}, {"name": None}],
            "name STRING",
        )
        result = df.filter(F.col("name").rlike(".*")).collect()
        assert len(result) == 1
        assert result[0]["name"] == "Alice"


class TestOrFilterWithIsNull:
    """Bug 3a: OR filter with isNull and equality."""

    def test_or_isnull_eq(self, spark):
        df = spark.createDataFrame(
            [
                {"url": None},
                {"url": ""},
                {"url": "http://example.com"},
            ],
            "url STRING",
        )
        result = df.filter((F.col("url").isNull()) | (F.col("url") == "")).collect()
        assert len(result) == 2

    def test_or_isnull_contains(self, spark):
        df = spark.createDataFrame(
            [
                {"url": None},
                {"url": "http://test.com"},
                {"url": "http://example.com"},
            ],
            "url STRING",
        )
        result = df.filter(
            (F.col("url").isNull()) | (F.col("url").contains("test"))
        ).collect()
        assert len(result) == 2


class TestPercentRank:
    """Bug 3b: percent_rank must match PySpark formula."""

    def test_percent_rank_basic(self, spark):
        df = spark.createDataFrame(
            [
                {"group": "A", "val": 1},
                {"group": "A", "val": 2},
                {"group": "A", "val": 3},
                {"group": "B", "val": 10},
            ],
            "group STRING, val INT",
        )
        w = Window.partitionBy("group").orderBy("val")
        result = df.withColumn("pr", F.percent_rank().over(w)).collect()
        pr_map = {(r["group"], r["val"]): r["pr"] for r in result}
        assert pr_map[("A", 1)] == 0.0
        assert pr_map[("A", 2)] == 0.5
        assert pr_map[("A", 3)] == 1.0
        assert pr_map[("B", 10)] == 0.0

    def test_percent_rank_single_partition(self, spark):
        df = spark.createDataFrame(
            [{"val": 5}],
            "val INT",
        )
        w = Window.orderBy("val")
        result = df.withColumn("pr", F.percent_rank().over(w)).collect()
        assert result[0]["pr"] == 0.0
