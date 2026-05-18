# MCI2026_Task2_Kelompok31

## A. Explanation of Database, Table Schema, and Pipeline:
Fetch.py code (Fey):

Process.py code (Fey & Isabel):

## B. Explanation of Insights, Visualization, and Dashboard:
Metabase visualization (Isabel):

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

## D. Conclusion and Afterthoughts
Previously we didn't know that source codes for the assignment were provided by the admins in the material folder, so we had to figure out and create the script ourselves 🥀🙏 Also, we assumed that the assignment asked for us to convert the data from the given API endpoint into an analytics-specific schema, so that's exactly what we did. 

