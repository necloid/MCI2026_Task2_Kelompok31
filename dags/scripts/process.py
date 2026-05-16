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

    print("📖 Membaca seluruh parquet orders dari Data Lake...")

    df_raw = spark.read.parquet(
        "/opt/airflow/data_lake/orders/"
    )

    print("✅ Data berhasil dibaca")

    print("\nPreview dataframe:")
    df_raw.show(5)

    # ANALYTICS PROCESSING
    print("\n📊 Menjalankan analytics processing...")

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

    print("\n🔄 Convert Spark DataFrame → Pandas...")

    top_products_pd = top_products_df.toPandas()
    hourly_orders_pd = hourly_orders_df.toPandas()
    department_pd = department_df.toPandas()

    spark.stop()

    print("✅ Spark processing selesai")
    print("\n🔌 Menghubungkan ke ClickHouse...")

    client = Client(
        host='clickhouse-server',

        user='admin',
        password='rahasia'
    )

    client.execute(
        'CREATE DATABASE IF NOT EXISTS analytics'
    )

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

    print("✅ Table ClickHouse siap")

    # TRUNCATE OLD DATA
    print("\n🧹 Membersihkan data lama...")

    client.execute('TRUNCATE TABLE analytics.top_products')

    client.execute('TRUNCATE TABLE analytics.hourly_orders')

    client.execute('TRUNCATE TABLE analytics.department_analytics')

    # INSERT DATA
    print("\n⬆️ Insert data ke ClickHouse...")

    # TOP PRODUCTS
    top_products_tuples = [
        tuple(x)
        for x in top_products_pd.to_numpy()
    ]

    if top_products_tuples:

        client.execute(
            'INSERT INTO analytics.top_products VALUES',
            top_products_tuples
        )

    # HOURLY ORDERS
    hourly_orders_tuples = [
        tuple(x)
        for x in hourly_orders_pd.to_numpy()
    ]

    if hourly_orders_tuples:

        client.execute(
            'INSERT INTO analytics.hourly_orders VALUES',
            hourly_orders_tuples
        )

    # DEPARTMENT ANALYTICS
    department_tuples = [
        tuple(x)
        for x in department_pd.to_numpy()
    ]

    if department_tuples:

        client.execute(
            'INSERT INTO analytics.department_analytics VALUES',
            department_tuples
        )

    print("✅ Semua analytics berhasil dimuat ke ClickHouse")
    print("\n🎉 Pipeline Orders Analytics Selesai!")

if __name__ == "__main__":
    run_orders_analytics()
