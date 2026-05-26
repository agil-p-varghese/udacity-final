from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Create contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read step trainer landing data
step_df = spark.read.json(
    "s3://agil-final-project/landing/step_trainer/"
)

# Read customer curated data
customer_df = spark.read.json(
    "s3://agil-final-project/curated/customer/"
)

# Join using serialNumber
step_trainer_trusted = step_df.join(
    customer_df,
    step_df["serialNumber"] == customer_df["serialNumber"],
    "inner"
).select(
    step_df["serialNumber"],
    step_df["sensorReadingTime"],
    step_df["distanceFromObject"]
)

# Write 
step_trainer_trusted.write.mode("overwrite").json(
    "s3://agil-final-project/trusted/step_trainer/"
)

print("step_trainer_trusted created successfully")
