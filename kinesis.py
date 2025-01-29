%python

from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

%sql
SET spark.databricks.delta.formatCheck.enabled=false

%python

###### read stream data ######
df = spark \
.readStream \
.format('kinesis') \
.option('streamName','Kinesis-Prod-Stream') \
.option('initialPosition','latest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

display(df)

###### filter and clean pin data ######
df_pin = df.filter(df.partitionKey == "df_pin")
df_pin = df_pin.selectExpr("CAST(data as STRING) jsonData")

struct = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True), 
    StructField("tag_list", StringType(), True),  
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", IntegerType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True),
])

df_pin = df_pin.select(from_json("jsonData", struct).alias("data")).select("data.*")

### clean dataframe: df_pin
# Replace empty entries and entries with no relevant data in each column with Nones
df_pin = df_pin.replace({'User Info Error': None})
# Transform the follower_count to ensure every entry is a number.
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k","000")).withColumn("follower_count", regexp_replace("follower_count", "M","000000"))
# Transform each column containing numeric data to numeric data type
df_pin = df_pin.withColumn("follower_count", col("follower_count").cast("int")).withColumn("downloaded", col("downloaded").cast("int")).withColumn("index", col("index").cast("int"))
# Rename the index column 
df_pin = df_pin.withColumnRenamed("index", "ind")
# Clean the data in the save_location column to include only the save location path
df_pin = df_pin.withColumn("save_location", regexp_extract(col("save_location"), r"/\S+", 0))
# reoder columns
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

display(df_pin)


###### filter and clean geo data ######
df_geo = df.filter(df.partitionKey == "df_geo")
df_geo = df_geo.selectExpr("CAST(data as STRING) jsonData")

struct = StructType([
    StructField("ind", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("country", StringType(), True),
])

df_geo = df_geo.select(from_json("jsonData", struct).alias("data")).select("data.*")

### clean dataframe: df_geo 
# Convert the timestamp column from a string to a timestamp data type
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))
# Create a new column coordinates that contains an array based on the latitude and longitude columns
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
# Drop the latitude and longitude columns from the DataFrame
df_geo = df_geo.drop("latitude", "longitude")
# Reorder the DataFrame columns
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

display(df_geo)


###### filter and clean user data ######
df_user = df.filter(df.partitionKey == "df_user")
df_user = df_user.selectExpr("CAST(data as STRING) jsonData")

struct = StructType([
    StructField("ind", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date_joined", TimestampType(), True),
])

df_user = df_user.select(from_json("jsonData", struct).alias("data")).select("data.*")

### clean dataframe: df_user 
# Create a new column user_name that concatenates the information found in the first_name and last_name columns
df_user = df_user.withColumn("user_name", concat("first_name", lit("  "), "last_name"))
# Drop the first_name and last_name columns from the DataFrame
df_user = df_user.drop("first_name", "last_name")
# Convert the date_joined column from a string to a timestamp data type
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))
#  Reorder the DataFrame columns
df_user = df_user.select("ind", "user_name", "age", "date_joined")

display(df_user)


###### write data to  Delta Table ######
dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

df_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .option("mergeSchema", "true") \
  .table("b82f438a9cb2_pin_table")

df_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .option("mergeSchema", "true") \
  .table("b82f438a9cb2_geo_table")

df_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .option("mergeSchema", "true") \
  .table("b82f438a9cb2_user_table")