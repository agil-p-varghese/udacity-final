from awsglue.context import GlueContext
from pyspark.context import SparkContext


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


step_df = spark.read.json(
    "s3://agil-final-project/landing/step_trainer/"
)


customer_df = spark.read.json(
    "s3://agil-final-project/curated/customer/"
)


step_trainer_trusted = step_df.join(
    customer_df,
    step_df["serialNumber"] == customer_df["serialNumber"],
    "inner"
).select(
    step_df["serialNumber"],
    step_df["sensorReadingTime"],
    step_df["distanceFromObject"]
)

 
step_trainer_trusted.write.mode("overwrite").json(
    "s3://agil-final-project/trusted/step_trainer/"
)

print("step_trainer_trusted created successfully")
