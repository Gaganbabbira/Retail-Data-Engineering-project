# Databricks notebook source
# DBTITLE 1,Retail Data Engineering Pipeline
                   ------------------------
                   #### BRONZE LAYER  #####
                   ------------------------- 
spark.conf.set(
    "fs.azure.account.key.adlsretail01.dfs.core.windows.net",
    "<YOUR_STORAGE_ACCESS_KEY>"
)

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://retail@adlsretail01.dfs.core.windows.net")
)

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://retail@adlsretail01.dfs.core.windows.net/bronze/")
)

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://retail@adlsretail01.dfs.core.windows.net/bronze/manish040596/AZURE-DATA-ENGINEER---DATABRICKS-PROJECTS/refs/heads/main/bronze_retail_data.parquet/")
)

# COMMAND ----------

# DBTITLE 1,RAW DATA INGESTION
                    ### RAW DATA INGESTION ###
                   -----------------------------
from pyspark.sql.functions import col, upper, trim, when
# Step 1: Read raw JSON (bronze)
bronze_df = spark.read.parquet("abfss://retail@adlsretail01.dfs.core.windows.net/bronze/manish040596/AZURE-DATA-ENGINEER---DATABRICKS-PROJECTS/refs/heads/main/bronze_retail_data.parquet/")
display(bronze_df)


# COMMAND ----------

# DBTITLE 1,Silver Layer
        -------------------------------------
        ### SILVER LAYER (DATA CLEANING) ###
        -------------------------------------
df_clean1 = bronze_df.filter(
    (col("TransactionID").isNotNull()) &
    (col("CustomerID").isNotNull()) &
    (col("TransactionDate").isNotNull())
)
display(df_clean1)


# COMMAND ----------

df_clean2 = df_clean1.withColumn("PaymentType", upper(trim(col("PaymentType")))) \
                     .withColumn("StoreRegion", upper(trim(col("StoreRegion")))) \
                     .withColumn("DeviceUsed", upper(trim(col("DeviceUsed"))))

display(df_clean2)


# COMMAND ----------

df_clean3 = df_clean2.withColumn("TransactionDate", col("TransactionDate").cast("timestamp")) \
                     .withColumn("Quantity", col("Quantity").cast("int")) \
                     .withColumn("Amount", col("Amount").cast("float")) \
                     .withColumn("Discount", col("Discount").cast("float"))
display(df_clean3)



# COMMAND ----------

df_clean4 = df_clean3.filter(
    (col("Quantity") > 0) & 
    (col("Amount") > 0) &
    (col("Discount") <= col("Amount")))
display(df_clean4)


# COMMAND ----------

df_clean5 = df_clean4.dropDuplicates(["TransactionID"])
display(df_clean5)


# COMMAND ----------

df_clean5.write.format("parquet").mode("overwrite").save("abfss://retail@adlsretail01.dfs.core.windows.net/silver/")

# COMMAND ----------

silver_df=spark.read.parquet('abfss://retail@adlsretail01.dfs.core.windows.net/silver/')
display(silver_df)

# COMMAND ----------

silver_df.createOrReplaceTempView('retail_data')

# COMMAND ----------

# MAGIC
# MAGIC  %sql
# MAGIC select * from retail_data

# COMMAND ----------

# DBTITLE 1,daily revenue by purchase
               --------------------------------------
               ### GOLD LAYER (BUSINESS METRICS) ###
               --------------------------------------
 %sql
 select date(TransactionDate),sum(amount) total_revenue,count(distinct TransactionID) total_purchase 
 from retail_data
 group by 1

# COMMAND ----------

# DBTITLE 1,revenue by payment type
# MAGIC
# MAGIC  %sql
# MAGIC   select sum(amount) total_revenue,PAYMENTTYPE from retail_data
# MAGIC  group by 2
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,store performance
# MAGIC
# MAGIC  %sql
# MAGIC  select sum(amount) total_revenue,STORELOCATION from retail_data
# MAGIC  group by 2

# COMMAND ----------

# DBTITLE 1,loyality level revenue contribution
# MAGIC
# MAGIC   %sql
# MAGIC  select sum(amount) total_revenue,CustomerLoyaltyLevel
# MAGIC   from retail_data
# MAGIC  group by 2

# COMMAND ----------

# DBTITLE 1,product category sales
# MAGIC
# MAGIC  %sql
# MAGIC select sum(amount) total_revenue,ProductCategory
# MAGIC   from retail_data
# MAGIC   group by 2

# COMMAND ----------

df_clean5.write.format("parquet").mode("overwrite").save("abfss://retail@adlsretail01.dfs.core.windows.net/gold/")



# COMMAND ----------

gold_df=spark.read.parquet('abfss://retail@adlsretail01.dfs.core.windows.net/gold/')
display(gold_df)