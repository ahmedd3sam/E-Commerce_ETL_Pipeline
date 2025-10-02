from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, when

spark = SparkSession.builder \
    .appName("Postgres to Hive ETL") \
    .enableHiveSupport() \
    .getOrCreate()

url = "jdbc:postgresql://host.docker.internal:5432/e_commerce"

properties = {
    "user": "external",
    "password": "external",
    "driver": "org.postgresql.Driver"
}
spark.conf.set("parquet.enable.dictionary", "false")
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
spark.conf.set("spark.sql.parquet.writeLegacyTimestamp", "true")

customer_df = spark.read.jdbc(url=url, table="customer", properties=properties)
product_df = spark.read.jdbc(url=url, table="product", properties=properties)
orders_df = spark.read.jdbc(url=url, table="orders", properties=properties)
logistics_df = spark.read.jdbc(url=url, table="logistics", properties=properties)
staging_orderlines_df = spark.read.jdbc(url=url, table="staging_orderlines", properties=properties)

customer_df.show()
customer_df.printSchema()

logistics_kpi = logistics_df.withColumn(
    "delivery_time_delta_days",
    when(
        (col("actual_delivery_date").isNotNull()) & (col("estimated_delivery_date").isNotNull()),
        datediff(col("actual_delivery_date"), col("estimated_delivery_date"))
    ).otherwise(None)
).withColumn(
    "is_late_delivery",
    when(
        (col("actual_delivery_date").isNotNull()) & 
        (col("estimated_delivery_date").isNotNull()) &
        (col("actual_delivery_date") > col("estimated_delivery_date")),
        1
    ).otherwise(0)
)

fact_df = staging_orderlines_df.alias("s") \
    .join(logistics_kpi.alias("l"), col("s.order_id") == col("l.order_id"), "inner") \
    .select(
        col("s.order_id"),
        col("s.customer_id"),
        col("s.product_id"),
        col("s.quantity"),
        col("s.total_price"),
        col("s.order_date"),
        col("s.shipping_cost"),
        col("l.logistics_id"),
        col("l.estimated_delivery_date"),
        col("l.actual_delivery_date"),
        col("l.warehouse_id"),
        col("l.delivery_time_delta_days"),
        col("l.is_late_delivery")
    )

spark.sql("CREATE DATABASE IF NOT EXISTS staging")

customer_df.write.mode("overwrite").saveAsTable("staging.customer")
product_df.write.mode("overwrite").saveAsTable("staging.product")
orders_df.write.mode("overwrite").saveAsTable("staging.orders")
logistics_kpi.write.mode("overwrite").saveAsTable("staging.logistics")
staging_orderlines_df.write.mode("overwrite").saveAsTable("staging.staging_orderlines")
fact_df.write.mode("overwrite").saveAsTable("staging.fact_orders_logistics")