# MCI2026_Task2_Kelompok31

## A. Explanation of Database, Table Schema, and Pipeline:
Fetch.py code (Fey):

Process.py code (Fey & Isabel):

This code does these main things:

### 1. Spark Context Initialization (initializing the SparkSession)
  
### 2. Data Ingestion (reading the data stored in a .parquet file directly from the data lake pathway /opt/airflow/data_lake/orders/)
   
### 3. Analytical Processing (processing the df_raw into specialized tables for calculating the analytics)

**a. Top Products:**


**b. Hourly Order Analytics:**


**c. Daily Orders Analytics:**

Similar to the previous field, this field is used to analyze purchasing behavior based on the days of the week. When we looked at the structure of the order_dow data format, we assume that it follows the format of 0=Sunday, 1=Monday, and so on.

`daily_orders_df = df_raw.groupBy(
    "order_dow"
).agg(
    F.countDistinct("order_id").alias("total_orders")
)`

`daily_orders_df = daily_orders_df.withColumn(
    "day_name",
    F.when(F.col("order_dow") == 0, "Sunday")
    .when(F.col("order_dow") == 1, "Monday")
    .when(F.col("order_dow") == 2, "Tuesday")
    .when(F.col("order_dow") == 3, "Wednesday")
    .when(F.col("order_dow") == 4, "Thursday")
    .when(F.col("order_dow") == 5, "Friday")
    .when(F.col("order_dow") == 6, "Saturday")
).orderBy("order_dow")`

`daily_orders_df = daily_orders_df.select(
    "order_dow",
    "day_name",
    "total_orders"
)`

- groups records based on order_dow 
- avoids counting multiple line items per order by calculating the distinct count of order_id
- uses conditionals to convert order_dow numbers into the corresponding name of day
- order everything by the order_dow
- re-order/confirm the structure of the data columns (so that there are no data order mismatches later on, I had a bug because of this)


**d. Department Analytics:**


**e. User Analytics:**
Tracks information in relation to the user, data, and orders. I suppose you can consider it like a customer profile.

`products_per_order_df = df_raw.groupBy(
    "user_id",
    "order_id"
).agg(
    F.count("*").alias("products_in_order")
)`

`user_analytics_df = products_per_order_df.groupBy(
    "user_id"
).agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.avg("products_in_order").alias("avg_products_per_order"),
    F.max("products_in_order").alias("largest_order_size"),
    F.min("products_in_order").alias("smallest_order_size"),
    F.sum("products_in_order").alias("total_products_bought")
).orderBy(F.desc("total_orders"))`

- calculates amount of products per order first by counting the amount of rows when grouping by user_id and order_id
- aggregates based on the product_per_order_df per user. Performs some calculations such as avg, max, min, and sum of products_in_order.


**f. User Loyalty:**
Calculates the user's loyalty based on the purchasing frequency and average days between orders.

`user_order_days_df = df_raw.groupBy("user_id").agg(
    F.avg("days_since_prior_order").alias("avg_days_between_orders"),
    F.countDistinct("order_id").alias("order_count")
)`

`user_loyalty_df = user_order_days_df.withColumn(
    "avg_days_between_orders",
    F.when(F.col("avg_days_between_orders").isNull(), 0.0)
    .otherwise(F.col("avg_days_between_orders"))
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
)`

- avg_days_between_orders: column that calculates the average of the days_since_prior_order field from the original data
- churn_risk_score: a normalized value between 0.0 and 0.1 which is calculated based on this formula: `churn_risk_score = 1 / total_orders * (1 + avg_days_between_orders / 30)`
- churn_risk: categorizes the churn_risk_score into the following:
  1. New Customer: Assigned if historical transactions equal 1 (order_count == 1), as there is not enough historical trend data to accurately evaluate customer behavior
  2. High Risk: Assigned to accounts with scores >= 0.7, flagging them for immediate retention marketing or discount offers
  3. Medium Risk: Assigned to accounts with scores between >= 0.4 and < 0.7
  4. Low Risk: Assigned to active, steady accounts with scores < 0.4



### 4. Data destructuring and Type Casting (converting PySpark's dataframes into Pandas and mapping the structure to Python tuples)

### 5. OLAP Layer Storage (initiates an external database driver connection to a distributed container network (host='clickhouse-server')




## B. Explanation of Insights, Visualization, and Dashboard:
Metabase visualization (Isabel):
<img width="1817" height="1013" alt="Screenshot 2026-05-18 220238" src="https://github.com/user-attachments/assets/04128c95-06c5-43ce-a7ae-17954a5e2b9c" />

<img width="2559" height="1245" alt="Screenshot 2026-05-18 220348" src="https://github.com/user-attachments/assets/814a15e5-e08e-4106-8d68-65105d3a5cc6" />

<img width="2559" height="1238" alt="Screenshot 2026-05-18 220529" src="https://github.com/user-attachments/assets/c0505834-b57a-45ca-b665-2212dbe8617d" />

<img width="2559" height="1248" alt="Screenshot 2026-05-18 220607" src="https://github.com/user-attachments/assets/a6e4ed6d-7cea-4da0-ad0b-2990a99f8f6b" />

<img width="1848" height="1238" alt="Screenshot 2026-05-18 220702" src="https://github.com/user-attachments/assets/42195510-8cd8-4be2-a198-4096b3dff163" />

<img width="1841" height="1249" alt="Screenshot 2026-05-18 220740" src="https://github.com/user-attachments/assets/75215c78-73dd-4bf2-9bd5-90e5553862d2" />


Some of the visualizations were made using SQL Queries from Metabase, but others simply used the Metabase Visualization tool (since most of the SQL Queries for the analytics database is already incorporated in the process.py code).

Full dashboard:

[Metabase - Product Analytics Dashboard.pdf](https://github.com/user-attachments/files/27967218/Metabase.-.Product.Analytics.Dashboard.pdf)



## C. Guide to Run Pipeline:

1. Build image using the command `docker-compose build` in our workspace location's terminal.

2. Install airflow database using `docker-compose up airflow-init`

3. Run the entire pipeline with `docker-compose up -d`

4. After waiting 1-2 minutes, open `http://localhost:8080` and login as `admin/admin`

5. run "Trigger DAG" (the play button)
<img width="1846" height="325" alt="Screenshot 2026-05-17 202017" src="https://github.com/user-attachments/assets/4bb4a64e-d99d-4e6e-bb85-0265a3faabf0" />

7. Enter the created ClickHouse database with the command `docker exec -it mci2026_task2_kelompok31-clickhouse-server-1 clickhouse-client`

8. Check the database using queries to see if everything works:
```
USE analytics;
SHOW TABLES;
```
<img width="789" height="1372" alt="Screenshot 2026-05-17 202626" src="https://github.com/user-attachments/assets/bfbf0bb5-d880-46f1-9a21-76719d793e33" />

`SELECT * FROM top_products`

<img width="1993" height="1375" alt="Screenshot 2026-05-17 201740" src="https://github.com/user-attachments/assets/92dfe106-4313-40f2-a5c2-5b535a3982f2" />

9. To run the script directly inside the container (for debugging purposes and seeing errors) use the command
`docker exec -it mci2026_task2_kelompok31-airflow-scheduler-1 bash -c "python //opt/airflow/dags/scripts/process.py"`

10. Open `http://localhost:3000` (currently figuring out how to share access between different devices), and sign in with the following information:

|Field|Value|
|-----|-----|
|Database type|ClickHouse|
|Display name	Orders|Data Warehouse|
|Host|clickhouse-server|
|Port|8123|
|Database name|analytics|
|Username|admin|
|Password|rahasia|

11. Go to New --> Question --> Data Warehouse --> choose any field you wish to visualize --> start visualizing

12. To synch your Metabase with the database schema, go to the Admin Settings --> Databases --> click on your specific ClickHouse database --> Sync database Schema

## D. Conclusion and Afterthoughts
Previously we didn't know that source codes for the assignment were provided by the admins in the material folder, so we had to figure out and create the script ourselves 🥀🙏 Also, we assumed that the assignment asked for us to convert the data from the given API endpoint into an analytics-specific schema, so that's exactly what we did. 

