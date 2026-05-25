from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Create contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read customer landing data
customer_df = spark.read.json(
    "s3://agil-final-project/landing/customer/"
)

# Filter customers who agreed to research
customer_trusted = customer_df.filter(
    col("shareWithResearchAsOfDate").isNotNull()
)

# Write trusted customer data
customer_trusted.write.mode("append").json(
    "s3://agil-final-project/trusted/customer/"
)

print("customer_trusted created successfully")
