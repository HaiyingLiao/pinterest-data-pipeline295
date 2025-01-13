bucket_name = "user-b82f438a9cb2-bucket"

df_pin = spark.read.load(f"s3a://{bucket_name}/topics/b82f438a9cb2.pin/partition=0/*.json",format="json")
df_geo = spark.read.load(f"s3a://{bucket_name}/topics/b82f438a9cb2.geo/partition=0/*.json",format="json")
df_user = spark.read.load(f"s3a://{bucket_name}/topics/b82f438a9cb2.user/partition=0/*.json",format="json")
display(df_user)