import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Create a simple DataFrame with sample data
data = [
    (1, "John", "Doe", 25, "Engineer"),
    (2, "Jane", "Smith", 30, "Data Scientist"),
    (3, "Bob", "Johnson", 35, "Manager"),
    (4, "Alice", "Williams", 28, "Analyst"),
    (5, "Charlie", "Brown", 32, "Developer")
]

# Define column names
columns = ["id", "first_name", "last_name", "age", "job_title"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Print DataFrame schema
print("DataFrame Schema:")
df.printSchema()

# Show DataFrame content
print("\nDataFrame Content:")
df.show()

# Show DataFrame with truncate=False to see full content
print("\nDataFrame Content (full):")
df.show(truncate=False)

# Print some basic statistics
print(f"\nNumber of rows: {df.count()}")
print(f"Number of columns: {len(df.columns)}")