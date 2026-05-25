from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Create contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read from Glue Catalog
step_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="project-db",
    table_name="step_trainer_trusted"
)

acc_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="project-db",
    table_name="accelerometer_trusted"
)

# Convert to DataFrames
step_df = step_dyf.toDF()
acc_df = acc_dyf.toDF()

# Print schemas
step_df.printSchema()
acc_df.printSchema()

# Join datasets
ml_curated = step_df.join(
    acc_df,
    step_df["sensorReadingTime"] ==
    acc_df["timestamp"],
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

# Write output
ml_curated.write.mode("overwrite").json(
    "s3://agil-final-project/curated/ml_curated/"
)

print("machine_learning_curated created successfully")
