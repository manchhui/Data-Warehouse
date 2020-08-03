# Project: Data Warehouse


## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms the data into a set of dimensional and fact tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## 1. Database Design Description
There are two source datasets, one called "song" and another "log". And from these two datasets the following star schema database will been created for optimized queries on song play analysis. The tables are as below:

### 1.1 Fact Table
The fact table in this star scheme will be named "songplays" and is designed to record "log" data associated with song plays. This fact table will have the
following columns: songplay_id (PK, SORTKEY), start_time (FK), user_id (FK), level, song_id (FK), artist_id (FK), session_id, location, user_agent. NOTE: PK denotes
PRIMARY KEY, FK denotes FORIEGN KEY and SORTKEY is to assist with optimsation of performance.

Additionally the fact table and all dimension tables have been distributed to all the available nodes with setting "DISTSTYLE ALL", to allow further optimation of query performance.
### 1.2 Dimension Tables
The following tables in this star scheme are all dimension tables.
- users - This table will be used to record unique user details. This table will have the following columns:
            user_id (PK, SORTKEY), first_name, last_name, gender, level.
- songs - This table will be used to record unique song details. This table will have the following columns:
            song_id (PK, SORTKEY), title, artist_id, year, duration.
- artists - This table will be used to record unique artist details. This table will have the following columns:
            artist_id (PK, SORTKEY), name, location, latitude, longitude.
- time - This table will be used to record unique time details. This table will have the following columns: 
            start_time (PK, SORTKEY), hour, day, week, month, year, weekday

### 1.3 Staging Tables, Dataset Cleanup and ETL process
The two source datasets, one called "song" and the other "log", resides on AWS S3 servers and in the ETL process these two datasets are:
- Extracted into two staging tables and then subseqently cleaned of NULL values from the following colummns:
    userId, sessionId, ts, song_id,  artist_id.
- Transformed, where the main data transformations are performed to allow data to be loaded into the:
    Songplays Table:
        - Duplicates removed.
        - "ts" variable is converted into a timestamp with the DATEADD function.
        - "song_id" and "artist_id" is added into a row of the table when the following variables are matched:
            staging_logs.song = songs.title
            staging_logs.artist = songs.name
            staging_logs.length = songs.duration
        - "songplay_id" created by using the CONCAT function on the "ts" variable and an INT created from the ROW_NUMBER() OVER function.
    Time Table:
        - "hour", "day", "week", "month", "year", "weekday" created using the DATE_PART function on start_time variable from the "songplays" table. 
- Loaded the extracted and transformed data into the fact and dimension tables mentioned above. Taking care to remove duplicates and only adding data that is new, with a special mention that "level" in the users table is updatable.
    

## 2. Files in the repository
There are two source datasets, one called "song" and another "log" and these are locationed in the data/song_data and data/log_data respectively. The subsections below 

### 2.1 create_tables.py
This script does the following:
- Establishes connection with the sparkify database on AWS and gets cursor to it.  
- Drops all the tables (by calling the script "sql_queries".  
- Creates all tables needed. 
- Finally, closes the connection.

### 2.2 sql_queries.py
This script does the following:  
- Drops (if exists) all the tables once called by the script "create_tables".  
- Creates all tables once called by the script "create_tables".
- Copies data from S3 into staging tables.
- Transform and insert data from staging tables into star schema tables.

### 2.3 etl.ipynb
This jupyter notedbook was used to develop the code used in the "etl.py" script.

### 2.4 dwh.cfg
This file contains configuration information that is required by "create_tables.py" and "etl.py" to access the AWS role previously created and perform ETL of data from S3 into the star schema tables.

### 2.5 etl.py
This script does the following:  
- Extracts, transforms and loads data into the tables already created in by the "create_tables.py" script. 


## 3. User Guide
To populated the tables with the data from the source datasets the following scripts "create_tables.py" and "etl.py" will have to be ran in sequence respectively in a terminal window, as per below:
- Type the following into the terminal window "Python create_tables.py", followed by the return key.
- Type the following into the terminal window "Python etl.py", followed by the return key.

After following the above two commands the database will be populated with the source data and ready for queries to be performed to extract the necessary data for analysis.
