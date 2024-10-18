from pyspark.sql import SparkSession
import os

# Setting up Spark session
spark = SparkSession.builder \
    .appName("customer data etl") \
    .getOrCreate()

# Azure SQL configurations
jdbc_url = "jdbc:sqlserver://<your-server-name>.database.windows.net:1433;database=<your-database-name>"
jdbc_properties = {
    "user": "<your-username>",
    "password": "<your-password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# Extracting data from Azure SQL
customer_df = spark.read.jdbc(url=jdbc_url, table="staging_customers", properties=jdbc_properties)


from pyspark.sql.functions import col, datediff, current_date, to_date, lit, expr

# transform the raw data as per requirement


transformed_df = customer_df.withColumn(
    "DOB", to_date(col("DOB"), "yyyyMMdd")
).withColumn(
    "age", datediff(current_date(), col("DOB")) / 365
).withColumn(
    "last_consultation_date", to_date(col("Last_Consulted_Date"), "yyyyMMdd")
).withColumn(
    "days_since_last_consultation", datediff(current_date(), col("last_consultation_date"))
).filter(col("days_since_last_consultation") > 30).filter(col("Customer_Id").isNull() | col("Country").isNull())

# Splitting data based on country
countries = transformed_df.select("Country").distinct().collect()

for country_row in countries:
    country_name = country_row['Country']
    country_df = transformed_df.filter(col("Country") == lit(country_name))



# Load each country-specific DataFrame into its own table in Azure SQL
country_table_name = f"Table_{country_name}"
country_df.write.jdbc(url=jdbc_url, table=country_table_name, mode="overwrite", properties=jdbc_properties)

