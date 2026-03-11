"""
PySpark snippet templates for robin-sparkless GitHub issue bodies.

Maps pattern key phrases to minimal PySpark code showing expected behavior.
Used by create_robin_sparkless_issues_from_parsed.py.
"""

# Pattern substring (in failure message) -> PySpark snippet
PYSPARK_SNIPPETS: dict[str, str] = {
    "cannot convert to Column": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
# PySpark accepts Column/str in select, filter, etc.
df.select(F.col("id"), "name").filter(F.col("id") > 1).show()""",

    "select expects Column or str": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "x"), (2, "y")], ["a", "b"])
df.select(F.col("a"), F.col("b").alias("b_alias")).show()""",

    "udf is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()
@F.udf(IntegerType())
def add_one(x):
    return x + 1 if x is not None else None

df = spark.createDataFrame([(1,), (2,)], ["x"])
df.select(F.col("x"), add_one(F.col("x")).alias("y")).show()""",

    "create_dataframe_from_rows failed": """from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType

spark = SparkSession.builder.getOrCreate()
# Struct
schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
spark.createDataFrame([(1, "x"), (2, "y")], schema).show()
# Map/array: PySpark accepts Row or tuple with proper schema
spark.createDataFrame([({"k": "v"}, [1, 2])], "m map<string,string>, arr array<int>").show()""",

    "explode is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(["a", "b", "c"],)], ["arr"])
df.select(F.explode(F.col("arr")).alias("item")).show()""",

    "casting from Utf8View to Boolean not supported": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("true",), ("false",), ("",)], ["s"])
# PySpark: string column in filter is cast to boolean where applicable
df.filter(F.col("s") == "true").show()""",

    "struct value must be object": """from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([StructField("id", StringType()), StructField("n", IntegerType())])
df = spark.createDataFrame([("A", 1), ("B", 2)], schema)
df.withColumn("struct", F.struct(F.col("id"), F.col("n"))).show()""",

    "DataFrames are not equivalent": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(2024, 3, 15)], ["y", "m", "d"])
# PySpark date/year/month etc. return consistent types
df.select(F.year(F.to_date(F.concat_ws("-", F.col("y"), F.col("m"), F.col("d")))).alias("year")).show()""",

    "struct is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df.select(F.struct(F.col("id"), F.col("name")).alias("s")).show()""",

    "array_contains": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([([1, 2, 3],), ([2, 4],)], ["arr"])
df.filter(F.array_contains(F.col("arr"), 2)).show()""",

    "arithmetic on string and numeric not allowed": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("5",), ("10",)], ["s"])
# PySpark: explicit cast for string/numeric ops
df.select(F.col("s").cast("int") + 1).show()""",

    "row_number is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["g", "v"])
w = Window.partitionBy("g").orderBy("v")
df.withColumn("rn", F.row_number().over(w)).show()""",

    "expr is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["a", "b"])
df.filter(F.expr("a > 1")).select(F.expr("upper(b)")).show()""",

    "invalid series dtype: expected `Boolean`": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 2.0), (2, 3.0)], ["a", "b"])
# PySpark: when/otherwise evaluates conditions as boolean
df.select(F.when(F.col("a") > 1, F.col("b")).otherwise(0)).show()""",

    "SQL failed: SQL: only SELECT": """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# PySpark SQL: INSERT, CREATE TABLE AS SELECT, etc. are supported
spark.sql("CREATE OR REPLACE TEMP VIEW t AS SELECT 1 AS x")
spark.sql("SELECT * FROM t").show()""",

    "posexplode is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(["x", "y"],)], ["arr"])
df.select(F.posexplode(F.col("arr"))).show()""",

    "join on expression": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "a")], ["id", "x"])
df2 = spark.createDataFrame([(1, "b")], ["id", "y"])
# PySpark: join on column expression
df1.join(df2, df1.id == df2.id).select(df1.id, df1.x, df2.y).show()""",

    "array_distinct is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([([1, 2, 1, 2],)], ["arr"])
df.select(F.array_distinct(F.col("arr")).alias("distinct_arr")).show()""",

    "SQL parse error": """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# PySpark: DESCRIBE, more SQL constructs supported
spark.sql("DESCRIBE TABLE EXTENDED default.t").show()""",

    "only INNER, LEFT, RIGHT, FULL JOIN": """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# PySpark: LEFT SEMI, LEFT ANTI, etc. supported
spark.sql("SELECT * FROM a LEFT SEMI JOIN b ON a.id = b.id")""",

    "array element": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([([[1, 2], [3]],)], ["arr_of_arr"])
df.select(F.col("arr_of_arr")[0]).show()""",

    "map column": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([({"a": 1, "b": 2},)], ["m"])
df.select(F.col("m")["a"].alias("val")).show()""",

    "over() is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 10), (1, 20), (2, 30)], ["g", "v"])
df.withColumn("first_v", F.first("v").over(Window.partitionBy("g").orderBy("v"))).show()""",

    "percent_rank is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 10), (1, 20), (2, 30)], ["g", "v"])
df.withColumn("pct", F.percent_rank().over(Window.partitionBy("g").orderBy("v"))).show()""",

    "to_timestamp is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("2023-01-15 12:00:00",)], ["s"])
df.select(F.to_timestamp(F.col("s")).alias("ts")).show()""",

    "rollup() is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a", 10), (1, "b", 20)], ["a", "b", "v"])
df.rollup("a", "b").agg(F.sum("v")).show()""",

    "cube() is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a", 10)], ["a", "b", "v"])
df.cube("a", "b").agg(F.sum("v")).show()""",

    "union failed: union:": """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
df2 = spark.createDataFrame([(2, "b")], ["id", "name"])  # same column names/order
df1.union(df2).show()""",

    "crossJoin() is not implemented": """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1,)], ["a"])
df2 = spark.createDataFrame([("x",)], ["b"])
df1.crossJoin(df2).show()""",

    "isnan is not implemented": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import math

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1.0,), (float("nan"),), (None,)], ["x"])
df.filter(F.isnan(F.col("x"))).show()""",

    "unsupported join type:": """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1,)], ["id"])
df2 = spark.createDataFrame([(1,)], ["id"])
# PySpark: LEFT SEMI / LEFT ANTI supported
df1.join(df2, "id", "leftsemi").show()""",

    "order_by failed:": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df.orderBy(F.col("id").desc()).show()""",

    "group_by failed:": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 10), (1, 20), (2, 30)], ["g", "v"])
df.groupBy("g").agg(F.sum("v").alias("total")).show()""",

    "select failed: not found: Column": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 10.0)], ["id", "value"])
# PySpark: agg alias available in select after groupBy
df.groupBy("id").agg(F.avg("value").alias("avg_value")).select("id", "avg_value").show()""",

    "with_column failed:": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, "a")], ["a", "b"])
df.withColumn("c", F.col("a").cast("string")).show()""",

    "agg failed:": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([(1, 10), (1, 20)], ["g", "v"])
df.groupBy("g").agg(F.count("*").alias("cnt")).show()""",

    "schema failed:": """from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("true",), ("false",)], ["s"])
df.select(F.col("s").cast("boolean")).show()""",

    "Database ": """from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("SHOW DATABASES").show()
spark.sql("USE some_db")""",
}


def get_pyspark_snippet(message: str) -> str:
    """Return a PySpark snippet for the given failure message, or a generic fallback."""
    msg = (message or "").strip()
    for key, snippet in PYSPARK_SNIPPETS.items():
        if key in msg:
            return snippet
    return """# PySpark accepts this operation and returns the expected result.
# Run the failing test with PySpark to see expected behavior:
#   MOCK_SPARK_TEST_BACKEND=pyspark pytest <test_id> -v

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
# Minimal example: create DataFrame and run the operation that fails under Robin
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
df.show()"""
