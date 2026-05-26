from awsglue.context import GlueContext
from pyspark.context import SparkContext


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


acc_df = spark.read.json(
    "s3://agil-final-project/trusted/accelerometer/"
)


customer_df = spark.read.json(
    "s3://agil-final-project/trusted/customer/"
)


customer_curated = acc_df.join(
    customer_df,
    acc_df["user"] == customer_df["email"],
    "inner"
).select(
    customer_df["birthDay"],
    customer_df["customerName"],
    customer_df["email"],
    customer_df["lastUpdateDate"],
    customer_df["phone"],
    customer_df["registrationDate"],
    customer_df["serialNumber"],
    customer_df["shareWithFriendsAsOfDate"],
    customer_df["shareWithPublicAsOfDate"],
    customer_df["shareWithResearchAsOfDate"]
).distinct()


customer_curated.write.mode("append").json(
    "s3://agil-final-project/curated/customer/"
)

print("customer_curated created successfully")
