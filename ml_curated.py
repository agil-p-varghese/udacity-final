from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col
# Create contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read trusted datasets
accelerometer_df = spark.read.json(
    "s3://agil-final-project/trusted/accelerometer/"
)

step_df = spark.read.json(
    "s3://agil-final-project/trusted/step_trainer/"
)
customer_df=spark.read.json(
    "s3://agil-final-project/trusted/customer/")
#step-customer
step_customer=step_df.join(
    customer_df,
    step_df["serialNumber"]==customer_df["serialnumber"],
    "inner"
    )
# Join with accelerometer using email and timestamp
ml_curated = step_customer.join(
    accelerometer_df,
    (step_customer["sensorReadingTime"] == accelerometer_df["timestamp"]) &
    (step_customer["email"]==accelerometer_df["user"]),
    "inner"
).select(
    step_customer["sensorReadingTime"],
    step_customer["serialNumber"],
    step_customer["distanceFromObject"],
    accelerometer_df["user"],
    accelerometer_df["x"],
    accelerometer_df["y"],
    accelerometer_df["z"],
    accelerometer_df["timestamp"]
)

# Write curated dataset
ml_curated.write.mode("append").json(
    "s3://agil-final-project/curated/ml_curated/"
)

print("ml_curated_data created successfully")
