# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from datathon.src.utils.data_transformation import 

# COMMAND ----------

# MAGIC %md
# MAGIC # Config & credentials

# COMMAND ----------

# Azure credentials
storage_account_name = dbutils.secrets.get(scope="azure_credentials", key="storage.account_name")
sas_token = dbutils.secrets.get(scope="azure_credentials", key="storage.sas_token")
container_name = "source-files"

# AWS credentials
aws_access_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.access_key")
aws_secret_key = dbutils.secrets.get(scope="aws_credentials", key="data_services.secret_key")

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)

# == S3 config
path_bucket = "neurum-ai-factored-datathon"
path_bronce_amz_reviews = f"s3a://{path_bucket}/bronce/amazon/reviews"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews

# COMMAND ----------

# path_bronce_invetory_data = f"s3a://{list_s3_credentials[0]}:{list_s3_credentials[1]}@{path_bucket_rg}/bronce/amazon/inventory_data"

# COMMAND ----------

path = "amazon_reviews/partition_2/"
df_partition_1_reviews = spark.read.format("json").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")

# COMMAND ----------

display(df_partition_1_reviews)

# COMMAND ----------

path = "amazon_metadata/partition_1/"
dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")

# COMMAND ----------

path = "amazon_metadata/partition_1/"
df_partition_1_metadata = spark.read.format("json").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")
display(df_partition_1_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

# df_all_partitions.explain(mode='cost')

# COMMAND ----------

df_all_partitions.coalesce(1).write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path_bronce_amz_reviews)


# COMMAND ----------

#df_partition_1_reviews.coalesce(1).write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path_bronce_amz_reviews)

# COMMAND ----------

df = df_partition_1_reviews

# COMMAND ----------

df.explain(mode='cost')

# COMMAND ----------

path = "amazon_reviews/"

path_list = dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")
print(len(path_list))

path_list

# COMMAND ----------

dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")

# COMMAND ----------

# Lista de paths
path_reviews = "amazon_reviews/"
paths = dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path_reviews}")

# DataFrame acumulativo
df_all_partitions = None

# Recorrer los paths y cargar los datos en el DataFrame acumulativo
for path_info in paths[:80]:
    path = f"amazon_reviews/{path_info.name}"
    print(path)
    df_partition = spark.read.format("json").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path}")

    if df_all_partitions is None:
        df_all_partitions = df_partition
    else:
        df_all_partitions = df_all_partitions.union(df_partition)

# COMMAND ----------

display(df_all_partitions)

# COMMAND ----------

# Save as table
# df_all_partitions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_db.amazon.reviews_sample")
