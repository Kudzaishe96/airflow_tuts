
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SuperstoreAnalysis") \
    .getOrCreate()

# Reference the file path inside the container's volume
df = spark.read.csv("/opt/airflow/data/The2014Inc.csv", header=True, inferSchema=True)

df.show(5)
print(f"Total Rows: {df.count()}")

spark.stop()