import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

input_path = "s3://your-bucket/input-folder/"
output_path = "s3://your-bucket/output-folder/"

input_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="landing-zone",
    table_name="lzreceipts"
)

spark_df = input_dyf.toDF()

spark_df = spark_df.withColumn("new_column", F.lit(5))

spark_df.show()


output_dyf = DynamicFrame.fromDF(spark_df, glueContext, "output_dyf")


s3_output_path = "s3://s3-bronze-training/test.parquet"

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": s3_output_path
    },
    format="parquet"
)

