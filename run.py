from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ModeFrequencyApp") \
        .config("spark.jars", "s3://sharadhakasi/extracredit/modefrequencyudaf_2.12-0.1.jar") \
        .getOrCreate()

    # Sample data
    data = [("a",), ("b",), ("b",), ("c",), ("c",),
            ("c",), ("d",), ("d",), ("d",), ("d",)]
    columns = ["value"]
    df = spark.createDataFrame(data, columns)

    # Register the UDAF
    spark.udf.registerJavaUDAF(
        "ModeFrequencyUDAF",
        "com.example.spark.udaf.ModeFrequencyUDAF"
    )

    # Use the UDAF
    result = df.select(expr("ModeFrequencyUDAF(value) as Mode_Frequency"))
    result.show(truncate=False)
