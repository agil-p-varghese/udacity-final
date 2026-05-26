from awsglue.context import GlueContext
from pyspark.context import SparkContext


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read trusted step trainer data
step_df = spark.read.json(
    "s3://agil-final-project/trusted/step_trainer/"
)

# Read trusted accelerometer data
acc_df = spark.read.json(
    "s3://agil-final-project/trusted/accelerometer/"
)

# Inner join on timestamp
ml_curated = step_df.join(
    acc_df,
    step_df["sensorReadingTime"] == acc_df["timestamp"],
    "inner"
).select(
    step_df["serialNumber"],
    step_df["sensorReadingTime"],
    step_df["distanceFromObject"],
    acc_df["user"],
    acc_df["x"],
    acc_df["y"],
    acc_df["z"],
    acc_df["timestamp"]
)


ml_curated.write.mode("overwrite").json(
    "s3://agil-final-project/curated/ml_curated/"
)

print("machine_learning_curated created successfully")
