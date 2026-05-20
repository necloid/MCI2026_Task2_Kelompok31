from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client

import os
import glob

def run_orders_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Analytics_Pipeline") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("📖 Reading all order parquet files from the Data Lake...")
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")
    print("✅ Data successfully loaded")

    # ANALYTICS PROCESSING
    print("\n📊 Running analytics processing...")

    # TOP PRODUCTS
    top_products_df = df_raw.groupBy(
        "product_id",
        "product_name",
        "department"
    ).agg(
        F.count("*").alias("total_purchases"),

        F.sum(
            F.when(F.col("reordered") == 1, 1)
            .otherwise(0)
        ).alias("total_reorders")

    ).orderBy(
        F.desc("total_purchases")
    ).limit(30)

    print("\n🔥 Top Products:")
    top_products_df.show(10)

    # HOURLY ORDER ANALYTICS
    hourly_orders_df = df_raw.groupBy(
        "order_hour_of_day"
    ).agg(
        F.countDistinct("order_id").alias("total_orders")
    ).orderBy(
        "order_hour_of_day"
    )

    print("\n🕒 Orders by Hour:")
    hourly_orders_df.show()

     # DAILY ORDERS ANALYTICS
    daily_orders_df = df_raw.groupBy(
        "order_dow"
    ).agg(
        F.countDistinct("order_id").alias("total_orders")
    )

    daily_orders_df = daily_orders_df.withColumn(
        "day_name",
        F.when(F.col("order_dow") == 0, "Sunday")
        .when(F.col("order_dow") == 1, "Monday")
        .when(F.col("order_dow") == 2, "Tuesday")
        .when(F.col("order_dow") == 3, "Wednesday")
        .when(F.col("order_dow") == 4, "Thursday")
        .when(F.col("order_dow") == 5, "Friday")
        .when(F.col("order_dow") == 6, "Saturday")
    ).orderBy("order_dow")

    daily_orders_df = daily_orders_df.select(
        "order_dow",
        "day_name",
        "total_orders"
    )

    print("\nOrders by Day:")
    daily_orders_df.show()

    # DEPARTMENT ANALYTICS
    department_df = df_raw.groupBy(
        "department"
    ).agg(
        F.count("*").alias("total_products_sold"),

        F.countDistinct("order_id").alias("unique_orders")

    ).orderBy(
        F.desc("total_products_sold")
    )

    print("\n🏬 Department Analytics:")
    department_df.show()

    # USER ANALYTICS
    # calculate amount of products inside each order
    products_per_order_df = df_raw.groupBy(
        "user_id",
        "order_id"
    ).agg(
        F.count("*").alias("products_in_order")
    )

    # aggregate per user
    user_analytics_df = products_per_order_df.groupBy(
        "user_id"
    ).agg(
        F.countDistinct("order_id").alias("total_orders"),

        F.avg("products_in_order")
            .alias("avg_products_per_order"),

        F.max("products_in_order")
            .alias("largest_order_size"),

        F.min("products_in_order")
            .alias("smallest_order_size"),

        F.sum("products_in_order")
            .alias("total_products_bought")

    ).orderBy(
        F.desc("total_orders")
    )

    print("\nUser Analytics:")
    user_analytics_df.show()

    # USER LOYALTY
    user_order_days_df = df_raw.groupBy("user_id").agg(
        F.avg("days_since_prior_order").alias("avg_days_between_orders"),
        F.countDistinct("order_id").alias("order_count")
    )

    user_loyalty_df = user_order_days_df.withColumn(
        "avg_days_between_orders",
        F.when(
            F.col("avg_days_between_orders").isNull(), 0.0
        ).otherwise(F.col("avg_days_between_orders"))
    ).withColumn(
        "churn_risk_score",
        F.least(
            F.lit(1.0),
            F.round(
                (F.lit(1.0) / F.col("order_count")) * (F.lit(1.0) + F.col("avg_days_between_orders") / F.lit(30.0)),
                2
            )
        )
    ).withColumn(
        "churn_risk",
        F.when(F.col("order_count") == 1, "New Customer")
        .when(F.col("churn_risk_score") >= 0.7, "High")
        .when(F.col("churn_risk_score") >= 0.4, "Medium")
        .otherwise("Low")
    ).select("user_id", "avg_days_between_orders", "churn_risk_score", "churn_risk")

    # churn_risk_score = 1 / total_orders * (1 + avg_days_between_orders / 30)
    # if the user only orders once, then avg_days_between_orders = 0

    # CONVERSION FROM DF TO PD
    print("\n🔄 Converting Spark DataFrames to Pandas...")

    top_products_pd = top_products_df.toPandas()
    hourly_orders_pd = hourly_orders_df.toPandas()
    daily_orders_pd = daily_orders_df.toPandas()
    department_pd = department_df.toPandas()
    user_analytics_pd = user_analytics_df.toPandas()
    user_loyalty_pd = user_loyalty_df.toPandas()
    
    spark.stop()

    print("✅ Spark processing complete")
    print("\n🔌 Connecting to ClickHouse...")

    client = Client(
        host='clickhouse-server',
        user='admin',
        password='rahasia'
    )

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')

    # TOP PRODUCTS TABLE
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.top_products (
            product_id UInt32,
            product_name String,
            department String,
            total_purchases UInt32,
            total_reorders UInt32
        )
        ENGINE = MergeTree()
        ORDER BY total_purchases
    ''')

    # HOURLY ORDERS TABLE
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.hourly_orders (
            order_hour_of_day UInt8,
            total_orders UInt32
        )
        ENGINE = MergeTree()
        ORDER BY order_hour_of_day
    ''')
    
    # DAILY ORDERS TABLES
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.daily_orders (
            order_dow UInt8,
            day_name String,
            total_orders UInt32
        )
        ENGINE = MergeTree()
        ORDER BY order_dow
    ''')

    # DEPARTMENT ANALYTICS TABLE
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.department_analytics (
            department String,
            total_products_sold UInt32,
            unique_orders UInt32
        )
        ENGINE = MergeTree()
        ORDER BY total_products_sold
    ''')

    # USER ANALYTICS TABLE
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.user_analytics (
            user_id UInt32,
            total_orders UInt32,
            avg_products_per_order Float64,
            largest_order_size UInt32,
            smallest_order_size UInt32,
            total_products_bought UInt32
        )
        ENGINE = MergeTree()
        ORDER BY total_orders
    ''')

    # USER LOYALTY TABLE
    client.execute('''
       CREATE TABLE IF NOT EXISTS analytics.user_loyalty (
        user_id UInt32,
        avg_days_between_orders Float64,
        churn_risk_score Float64,
        churn_risk String
    )
    ENGINE = MergeTree()
    ORDER BY user_id
    ''')

    print("✅ ClickHouse tables are ready")

    # TRUNCATE OLD DATA
    print("\n🧹 Cleaning up old data...")
    client.execute('TRUNCATE TABLE analytics.top_products')
    client.execute('TRUNCATE TABLE analytics.hourly_orders')
    client.execute('TRUNCATE TABLE analytics.daily_orders')
    client.execute('TRUNCATE TABLE analytics.department_analytics')
    client.execute('TRUNCATE TABLE analytics.user_analytics')
    client.execute('TRUNCATE TABLE analytics.user_loyalty')

    # INSERT DATA
    print("\n⬆️ Inserting data into ClickHouse...")

    # TOP PRODUCTS INSERT
    top_products_tuples = [tuple(x) for x in top_products_pd.to_numpy()]
    if top_products_tuples:
        client.execute('INSERT INTO analytics.top_products VALUES', top_products_tuples)

    # HOURLY ORDERS INSERT
    hourly_orders_tuples = [tuple(x) for x in hourly_orders_pd.to_numpy()]
    if hourly_orders_tuples:
        client.execute('INSERT INTO analytics.hourly_orders VALUES', hourly_orders_tuples)

    # DAILY ORDERS INSERT
    daily_orders_tuples = [tuple(x) for x in daily_orders_pd.to_numpy()]
    if daily_orders_tuples:
        client.execute('INSERT INTO analytics.daily_orders VALUES', daily_orders_tuples)

    # DEPARTMENT ANALYTICS INSERT
    department_tuples = [tuple(x) for x in department_pd.to_numpy()]
    if department_tuples:
        client.execute('INSERT INTO analytics.department_analytics VALUES', department_tuples)

    # USER ANALYTICS INSERT
    user_analytics_tuples = [
        (int(row.user_id), int(row.total_orders), round(float(row.avg_products_per_order), 2), int(row.largest_order_size), int(row.smallest_order_size), int(row.total_products_bought))
        for row in user_analytics_pd.itertuples(index=False)]
    if user_analytics_tuples:
        client.execute('INSERT INTO analytics.user_analytics VALUES', user_analytics_tuples)

    # USER LOYALTY INSERT
    user_loyalty_tuples = [
        (int(row.user_id), round(float(row.avg_days_between_orders or 0), 2), round(float(row.churn_risk_score), 2), str(row.churn_risk))
        for row in user_loyalty_pd.itertuples(index=False)]
    if user_loyalty_tuples:
        client.execute('INSERT INTO analytics.user_loyalty VALUES', user_loyalty_tuples)

    print("✅ All analytics data successfully loaded into ClickHouse")
    
    # CLEANUP
    print("Cleaning up old parquet files from Data Lake...")
    files = glob.glob('/opt/airflow/data_lake/orders/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")

    print("\n🎉 Orders Analytics pipeline completed!")

if __name__ == "__main__":
    run_orders_analytics()
