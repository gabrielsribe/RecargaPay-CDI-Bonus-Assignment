# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, TimestampType, StringType, LongType
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

try:
  df_transactions = spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/gabrielsribe@gmail.com/*.parquet")
except:
  print("Failed to read the data!")
  raise Exception("Failed to read the data!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning and Checking Data
# MAGIC It has been verified that I have a wallet creation event for each account_id

# COMMAND ----------

#Adjust schema
df_transactions_cleaned = df_transactions \
    .withColumn("event_time", F.col("event_time").cast(TimestampType())) \
    .withColumn("user_id", F.col("user_id").cast(StringType())) \
    .withColumn("account_id", F.col("account_id").cast(StringType())) \
    .withColumn("amount", F.col("amount").cast(DecimalType(10, 2))) \
    .withColumn("transaction_type", F.col("transaction_type").cast(StringType())) \
    .withColumn("cdc_operation", F.col("cdc_operation").cast(StringType())) \
    .withColumn("cdc_sequence_num", F.col("cdc_sequence_num").cast(LongType())) \
    .withColumn("source_system", F.col("source_system").cast(StringType()))

# COMMAND ----------

#Data quality:
errors = []
if df_transactions_cleaned.filter(F.col("user_id").isNull()).count() > 0:
    errors.append("Column ‘user_id’ has null values.")

if df_transactions_cleaned.filter(F.col("account_id").isNull()).count() > 0:
    errors.append("Column ‘account_id’ has null values.")

if df_transactions_cleaned.filter(F.col("amount").isNull()).count() > 0:
    errors.append("Column ‘amount’ has null values.")

if df_transactions_cleaned.filter(F.col("event_time").isNull()).count() > 0:
    errors.append("Column ‘event_time’ has null values.")

if df_transactions_cleaned.filter(F.col("cdc_sequence_num").isNull()).count() > 0:
    errors.append("Column ‘cdc_sequence_num’ has null values.")

if df_transactions_cleaned.filter(~ F.col("transaction_type")\
    .isin("TRANSFER_OUT","DEPOSIT","WITHDRAWAL","TRANSFER_IN","WALLET_CREATED")).count() > 0:
    errors.append("Column transaction_type has null values.")

if errors:
    error_message = "\n".join(errors)
    raise Exception(f"Failed data quality validation:\n{error_message}")
else:
    print("Dataset validated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Intermediate table: User balance over time

# COMMAND ----------

# Get only necessary events
df_valid_events = df_transactions_cleaned.filter(F.col("cdc_operation").isin("insert", "update")) \
    .withColumn("date", F.to_date("event_time"))

# COMMAND ----------

# Group transactions by account_id and date
df_daily_movements = (
    df_valid_events.groupBy("account_id", "date")
    .agg(F.sum("amount").alias("daily_amount"))
)

# COMMAND ----------

#Get date range from data
date_range = df_daily_movements.select(F.min("date").alias("start"), F.max("date").alias("end")).collect()[0]

#Select start date and end date
start_date, end_date = date_range["start"], date_range["end"]

#Generate Dataframe with the dates
def generate_date_range(start, end):
    days = (end - start).days + 1
    return [(start + timedelta(days=i),) for i in range(days)]

df_dates = spark.createDataFrame(
    generate_date_range(start_date, end_date),
    ["date"]
)

# COMMAND ----------

# Cross Join between account_id and dates
df_accounts = df_valid_events.select("account_id").distinct()
df_accounts_with_dates = df_accounts.crossJoin(df_dates)

# COMMAND ----------

#Join with daily movement data, filling in null values with zero
df_accounts_with_dates_daily_movements = (
    df_accounts_with_dates
    .join(df_daily_movements, on=["account_id", "date"], how="left")
    .withColumn("daily_amount", F.coalesce(F.col("daily_amount"), F.lit(0.0)))
)

# COMMAND ----------

#Calculate accumulated balance by account_id
window_spec = Window.partitionBy("account_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_daily_balance = df_accounts_with_dates_daily_movements.withColumn("daily_balance", F.sum("daily_amount").over(window_spec))

# COMMAND ----------

#Daily Balance dataset Data Quality:
errors = []
if df_daily_balance.filter(F.col("date").isNull()).count() > 0:
    errors.append("Column date has null values.")

if df_daily_balance.filter(F.col("account_id").isNull()).count() > 0:
    errors.append("Column ‘account_id’ has null values.")

if df_daily_balance.filter((F.col("daily_amount").isNull())).count() > 0:
    errors.append("Column cdi_interest_value has null values.")

if df_daily_balance.filter((F.col("daily_balance").isNull())).count() > 0:
    errors.append("Column daily_balance has null values.")

if errors:
    error_message = "\n".join(errors)
    raise Exception(f"Failed data quality validation:\n{error_message}")
else:
    print("Dataset validated successfully!")

# COMMAND ----------

# Result
df_daily_balance.filter("account_id = '001a008c-031f-5c97-9f5f-6369a53b4b4f'").display()

# COMMAND ----------

#Select only necessary columns
df_daily_balance = df_daily_balance.select("date", "account_id", "daily_balance")
df_daily_balance = df_daily_balance.withColumnRenamed("daily_balance", "current_balance")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Table logic: CDI Bonus

# COMMAND ----------

# MAGIC %md
# MAGIC ### Previous Day Balance

# COMMAND ----------

#Generate column with previous day's balance
window_spec = Window.partitionBy("account_id").orderBy("date")
df_wallet_balance_cdi = df_daily_balance.withColumn("previous_balance", F.lag("current_balance").over(window_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unchanged Balance Value

# COMMAND ----------

#Calculate how much of the balance over 100 has not changed in the last 24 hours
df_wallet_balance_cdi_unchanged_value = df_wallet_balance_cdi.withColumn(
    "balance_above_100_for_one_day",
    F.greatest(
        F.lit(0),
        F.least(
            F.when(F.col("current_balance") > 100, F.col("current_balance") - 100).otherwise(0),
            F.when(F.col("previous_balance") > 100, F.col("previous_balance") - 100).otherwise(0)
        )
    )
)

# COMMAND ----------

df_wallet_balance_cdi_unchanged_value.filter("date >= '2024-09-29'").filter("account_id = '001a008c-031f-5c97-9f5f-6369a53b4b4f'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply CDI Bonus

# COMMAND ----------

#Daily CDI calculation, simulating values
df_cdi = spark.createDataFrame([
    ("2024-10-01", 0.0015),
    ("2024-10-02", 0.0014),
    ("2024-10-03", 0.0015),
    ("2024-10-04", 0.0016),
    ("2024-10-05", 0.0014),
    ("2024-10-06", 0.0015),
    ("2024-10-07", 0.0016),
    ("2024-10-08", 0.0014),
    ("2024-10-09", 0.0015),
    ("2024-10-10", 0.0016),
    ("2024-10-11", 0.0014),
    ("2024-10-12", 0.0015),
    ("2024-10-13", 0.0016),
    ("2024-10-14", 0.0017),
    ("2024-10-15", 0.0014),
    ("2024-10-16", 0.0015),
    ("2024-10-17", 0.0016),
    ("2024-10-18", 0.0017),
    ("2024-10-19", 0.0014),
    ("2024-10-20", 0.0016),
], ["date", "daily_cdi_rate"]).withColumn("date", F.to_date("date"))

# COMMAND ----------

#Join ballance dataset with cdi dataset
df_ballance_cdi = df_wallet_balance_cdi_unchanged_value.join(df_cdi, on=["date"], how="left")

#Generate the CDI interest value
df_bonus_cdi = df_ballance_cdi.withColumn(
    "cdi_interest_value", F.col("balance_above_100_for_one_day") * F.col("daily_cdi_rate")
).select("date", "account_id", "balance_above_100_for_one_day", "cdi_interest_value")

#As in our simulation we only have the CDI value for the month of 2024-10-* , we will filter the table records to only include this month. Ideally, we would have the entire historical CDI record
df_bonus_cdi = df_bonus_cdi.filter((F.col("cdi_interest_value").isNotNull()))

# COMMAND ----------

#Data CDI dataset quality:
errors = []
if df_bonus_cdi.filter(F.col("date").isNull()).count() > 0:
    errors.append("Column date has null values.")

if df_bonus_cdi.filter(F.col("account_id").isNull()).count() > 0:
    errors.append("Column ‘account_id’ has null values.")

if df_bonus_cdi.filter((F.col("cdi_interest_value").isNull()) | (F.col("cdi_interest_value") < 0)).count() > 0:
    errors.append("Column cdi_interest_value has null or negative values.")

if errors:
    error_message = "\n".join(errors)
    raise Exception(f"Failed data quality validation:\n{error_message}")
else:
    print("Dataset validated successfully!")

# COMMAND ----------

#Result
df_bonus_cdi.filter("account_id = '001a008c-031f-5c97-9f5f-6369a53b4b4f'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Suggestion on how to add this information to a relational database

# COMMAND ----------

#Generating dataset with only CDI amounts to be deposited (round value)
df_payout = df_bonus_cdi.filter(F.col("cdi_interest_value") > 0)\
    .withColumn("transaction_type", F.lit("cdi_interest")) \
    .withColumn("amount", F.round(F.col("cdi_interest_value"), 2)) \
    .withColumn("source_system", F.lit("cdi_bonus_system")) \
    .withColumn("event_time", F.current_timestamp()) \
    .withColumn("user_id", F.lit("system_user_id")) \
    .select("event_time", "user_id", "account_id", "amount", "transaction_type", "source_system")

df_payout.display()

# COMMAND ----------

#Connection configuration, ideally get this information from the vault
jdbc_url = "my_jdbc_url"
dbtable = "my_dbtable"
user_name = "USER"
password = "PASSWORD"

# COMMAND ----------

#Write to database
df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", db_table) \
    .option("user", "USER") \
    .option("password", "PASSWORD") \
    .mode("append") \
    .save()