# Project: Data Warehouse
## Contents

+ [Project Discription](#Project-Discription)
+ [Project Datasets](#Project-Datasets)
+ [ETL Process](#ETL-Process)
+ [Database](#Database)
+ [Project Files](#Project-Files)
+ [How To Run](#How-To-Run)

---

### Project Discription

In this project I built an ETL pipeline that extracts data from Amazon's S3, staged them in Redshift, and transformed data into a set of dimensional tables for analytics team to continue finding insights into what songs users are listening to. 

![alt text](Images/S3_to_Redshift_ETL.png)

---

## Project Datasets

* Song data: 's3://udacity-dend/song_data'  
* Log data: 's3://udacity-dend/log_data'

---

### ETL Process

Each Dataset was copied into a table created in the Redshift Cluster, and then these tables were used to insert values into the Star schema, so that the data can be ready for analysis later.

---
### Database 
The database design schema consists of the following tables:
<details>
<summary>
Staging_events Table
</summary>

> Staging table contains the data copied from the S3 log data.

![alt text](Images/staging_events_table.png)

</details>

<details>
<summary>
Staging_songs Table
</summary>

> Staging table contains the data copied from the S3 song data.

![alt text](Images/staging_songs_table.png)

</details>

<details>
<summary>
Songplay Table
</summary>

> This is the fact table for the Star Schema that will be used for analysis.

![alt text](Images/songplay_table.png)
</details>

<details>
<summary>
Songs Table
</summary>

> Dimension table that contains details on songs from song files.

![alt text](Images/song_table.png)
</details>

<details>
<summary> Artists Table </summary>

> Dimension table that contains details on artist from song files.

![alt text](Images/artist_table.png)
</details>

<details>
<summary> Users Table </summary>

> Dimension table that contains data on sparkify users derived from log files.

![alt text](Images/users_table.png)
</details>

<details>
<summary> Time Table </summary>

> Dimension table that contains a list of timestamps and converted time data from log files.

![alt text](Images/time_table.png)
</details>


---

### Project Files 
This project consists of the following files:
+ `sql_queries.py` - This file contains Postgres SQL queries in string formate. 
+ `create_tables.py` - This script uses the sql_queries.py file to create new tables or drop old tables in the database.
+ `etl.py` - This script is used to build ETL processes which will read every file contained S3 bucket, copy its data into tables in the Redshift Cluster, then insert its values into the Star Schema using variables in sql_queries.py file.
+ `dwh.cfg` - This File contains the IAM role ARN, the path to S3 Datasets and the Redshift Cluster configurations.
+ `test.ipynb` - This notebook is used for testing purposes after finishing, to run queries on (you can also run queries in AWS Redshift query editor). 

---

### How To Run

Firstly, you need to create IAM Role that has read access to S3 bucket, then you need to create the Redshift Cluster and assosiate the IAM role to it. After that, you need to fill in the IAM role ARN and the Redshift Cluster configurations into the `dwh.cfg` file. Finally run `create_tables.py` file to drop and create the tables and then run `etl.py` to insert the data into the tables.


