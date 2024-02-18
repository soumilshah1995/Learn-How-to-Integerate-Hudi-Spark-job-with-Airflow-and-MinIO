from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .getOrCreate()

# Print Spark version
print("Spark version:", spark.version)

# Create a simple DataFrame
data = [("Hello", 1), ("World", 2), ("!", 3)]
df = spark.createDataFrame(data, ["word", "count"])

# Print the DataFrame to the console
df.show()

# Stop the SparkSession
spark.stop()
