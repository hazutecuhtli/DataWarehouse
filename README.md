# Udacity - Data Warehouse Project

## Introduction

A music streaming startup, Sparkify, has grown its user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. It is required to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Sparkify databases

### Songs Database

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

### Users Database

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

## Schema for Song Play Analysis

Using the song and event datasets, It is required to create a star schema optimized for queries on song play analysis, including the following tables:

### Fact Table

**songplays** - *records in event data associated with song plays i.e. records with page NextSong*:
-songplay_id
-start_time
-user_id
-level
-song_id
-artist_id
-session_id
-location
-user_agent

### Dimension Tables

**users** - *users in the app*:
-user_id
-first_name
-last_name
-gender
-level

**songs** - *songs in music database*:
-song_id
-title
-artist_id
-year
-duration

**artists** - *artists in music database*:
-artist_id
-name
-location
-lattitude
-longitude

**time** - *timestamps of records in songplays broken down into specific units*:
-start_time
-hour
-day
-week
-month
-year
-weekday

## Project Files Content

The following files compose the solution for the presented problem, each of them is explained briefly next:

**sql_queries.py:**

This file contains SQL instructions to create the required database, with the defined schema and tables. It also contains SQL instructions for queries to fill the created tables that compose the database schema. 

**create_table.py :**

This python script runs functions to drop, in case exists, and create tables that generate the desired database schema.

**etl.py:**

Python script that runs the extract, transform and load (ETL)  routines to fill the required schema.

**dwf.cfg:**

Files that contains information about to create a AWS *Redshift* cluster and *IAM* role, to have access to the Sparkify S3 database. 

### Running the files

In order to run the files, it is necessary to correctly fill the information of the AWS Redshift cluster and the IAM role. Then, the create_tables.py script to drop any existed database of interest and to create the required tables. Finally, the etl.py script is used to fill data to the created tables.

## Database Schema Design and ETL Pipelines

The required schema presented in the *Schema for Song Play Analysis* section is fulfilled as presented in the *sql_queries.py* file. All required tables were created accordingly to the Sparkify S3 datasets, using VARCHAR and TEXT variables types to represent strings and timestamps and INTEGERS to represent numerical variables. The star schema was achieved using Primary keys on all created tables and using the references keyword to link two tables together. 

Therefore, the star schema is achieved by creating the songplays fact table and by linked the user_id, song_id artist_id and start_time dimensions to the users, songs, artists and time dimensional tables, respectively. Additionally, the ETL pipelines are used to avoid loading null and repeated data to the created tables that consist of the mentioned schema. This was possible using the DISTINCT and NOT NULL SQL keywords and by using SQL sub QUERIES. Furthermore, the provided timestamp from the Sparkyfi S3 database was split into different time formats to create the time SQL table, using the EXTRACT SQL keyword. 

## Examples of Queries

### Songplay table Query

*select * from songplays
order by songplay_id
limit 10;*

![alt text](/images/SongPlayQuery.png)

### Users table Query

*select * from users
order by user_id
limit 10;*

![alt text](/images/UsersQuery.png)

### Songs table Query

*select * from songs
order by song_id
limit 10;*

![alt text](/images/SongsQuery.png)

### Artists table Query

*select * from artists
order by artist_id
limit 10;*

![alt text](/images/ArtistsQuery.png)

### Time table Query

*select * from time
order by start_time
limit 10;*

![alt text](/images/TimeQuery.png)