from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

customer_df = spark.read.json(
    "s3://agil-final-project/landing/customer/"
)


customer_trusted = customer_df.filter(
    col("shareWithResearchAsOfDate").isNotNull()
)


customer_trusted.write.mode("append").json(
    "s3://agil-final-project/trusted/customer/"
)

print("customer_trusted created successfully")
