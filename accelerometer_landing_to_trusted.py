from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Create contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


accelerometer_df = spark.read.json(
    "s3://agil-final-project/landing/accelerometer/"
)


customer_df = spark.read.json(
    "s3://agil-final-project/trusted/customer/"
)


accelerometer_trusted = accelerometer_df.join(
    customer_df,
    accelerometer_df["user"] == customer_df["email"],
    "inner"
).select(
    accelerometer_df["user"],
    accelerometer_df["timestamp"],
    accelerometer_df["x"],
    accelerometer_df["y"],
    accelerometer_df["z"]
)


accelerometer_trusted.write.mode("append").json(
    "s3://agil-final-project/trusted/accelerometer/"
)

print("accelerometer_trusted created successfully")
