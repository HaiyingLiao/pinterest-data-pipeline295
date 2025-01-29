from pyspark.sql.functions import regexp_replace, col, regexp_extract, to_timestamp, array, concat, lit, count, year, length,expr,when, median
from pyspark.sql.window import Window

bucket_name = "user-b82f438a9cb2-bucket"

df_pin = spark.read.load(f"s3a://{bucket_name}/topics/b82f438a9cb2.pin/partition=0/*.json",format="json")
df_geo= spark.read.load(f"s3a://{bucket_name}/topics/b82f438a9cb2.geo/partition=0/*.json",format="json")
df_user= spark.read.load(f"s3a://{bucket_name}/topics/b82f438a9cb2.user/partition=0/*.json",format="json")

########## clean DataFrames ##########
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

### clean dataframe: df_geo 
# Convert the timestamp column from a string to a timestamp data type
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))
# Create a new column coordinates that contains an array based on the latitude and longitude columns
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
# Drop the latitude and longitude columns from the DataFrame
df_geo = df_geo.drop("latitude", "longitude")
# Reorder the DataFrame columns
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

### clean dataframe: df_user 
# Create a new column user_name that concatenates the information found in the first_name and last_name columns
df_user = df_user.withColumn("user_name", concat("first_name", lit("  "), "last_name"))
# Drop the first_name and last_name columns from the DataFrame
df_user = df_user.drop("first_name", "last_name")
# Convert the date_joined column from a string to a timestamp data type
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))
#  Reorder the DataFrame columns
df_user = df_user.select("ind", "user_name", "age", "date_joined")
####################################

# task 4 : Find the most popular Pinterest category people post to based on their country.
joined_df = df_pin.join(df_geo, df_pin['ind'] == df_geo['ind'], how="left")
joined_df = joined_df.groupBy("country", "category").agg(count("*").alias("category_count")).orderBy("country", "category_count", ascending=False)

# task 5 : Find how many posts each category had between 2018 and 2022.
joined_df = df_pin.join(df_geo, df_pin['ind'] == df_geo['ind'], how="left")
joined_df = joined_df.withColumn("post_year", year(col("timestamp")))
joined_df = joined_df.filter((col("post_year") >= 2018) & (col("post_year") <= 2022))
joined_df = joined_df.groupBy("post_year", "category").agg(count("*").alias("category_count")).orderBy("post_year", "category_count", ascending=False)

# task 6 :
joined_df = df_pin.fillna({"follower_count": 0})
joined_df = df_pin.join(df_geo, df_pin['ind'] == df_geo['ind'], how="left")
joined_df = joined_df.groupBy("country", "poster_name").agg({"follower_count": "sum"}).withColumnRenamed("sum(follower_count)", "follower_count")
joined_df = joined_df.orderBy( "follower_count",ascending=False)

# task 7 :
joined_df = df_pin.join(df_user, df_pin['ind'] == df_user['ind'], how="left")
joined_df = joined_df.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .otherwise("50+")  # If age > 50
)
joined_df = joined_df.groupBy("category", "age_group").agg(count("*").alias("category_count")).orderBy("age_group","category_count", ascending=False)

# task 8 :
joined_df = df_pin.join(df_user, df_pin['ind'] == df_user['ind'], how="left")
joined_df = joined_df.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .otherwise("50+")  # If age > 50
)
joined_df = joined_df.groupBy("age_group").agg(median("follower_count").alias("median_follower_count")).orderBy("age_group")

# task 9 :
df_user = df_user.withColumn("date_joined", year(col("date_joined"))).filter((col("date_joined") >= 2015) & (col("date_joined") <= 2020))
df_user = df_user.groupBy("date_joined").agg(count("*").alias("number_users_joined")).orderBy("date_joined","number_users_joined")

# task 10 :
joined_df = df_pin.join(df_user, df_pin['ind'] == df_user['ind'], how="left")
joined_df = joined_df.withColumn("date_joined", year(col("date_joined"))).filter((col("date_joined") >= 2015) & (col("date_joined") <= 2020))
joined_df = joined_df.groupBy("date_joined").agg(median("follower_count").alias("median_follower_count"))

# task 11 :
joined_df = df_pin.join(df_user, df_pin['ind'] == df_user['ind'], how="left")
joined_df = joined_df.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .otherwise("50+")  # If age > 50
)
joined_df = joined_df.withColumn("date_joined", year(col("date_joined"))).filter((col("date_joined") >= 2015) & (col("date_joined") <= 2020))
joined_df = joined_df.groupBy("age_group").agg(median("follower_count").alias("median_follower_count"))