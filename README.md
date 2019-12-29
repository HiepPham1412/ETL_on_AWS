# Puspose

This project aims to build a simple ETL process to transform the raw data in json format stored in S3 to proper database in Redshift. The database should be able to answer analytical questions for online music streaming company such as "What is the most played song over past 24 hours?", "Who is the most popular artist during this year?"

In particular, there are main steps:
1. Create a STAR schema database
2. Load the raw data to staging tables
3. Transform the data in staging tables and  insert to STAR schema database

# Raw data
`song_data` is stored in a S3 bucket: s3://udacity-dend/song_data. This data in json format and contain all the basic information of each song {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0} 

`log_data` is stored in a S3 bucket: s3://udacity-dend/log_data. It contains all the information in relation to the song played by users such as artist, auth, firsName, lastName, length, song,..


# Data base schema

The database contains following tables:
**Fact table**: 
`songplays` table  records in event data associated with song plays. 
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

**Dimension Tables**
`users` - users in the app
*user_id, first_name, last_name, gender, level*

`songs` - songs in music database
*song_id, title, artist_id, year, duration*

`artists` - artists in music database
*artist_id, name, location, lattitude, longitude*

`time` - timestamps of records in songplays broken down into specific units
*start_time, hour, day, week, month, year, weekday*

# How to?

`Test ETL implementation.ipynb` notebooks to test the whole ETL process  
`create_tables.py`: script to create data tables in Redshift cluster 
`etl.py`: script to do ETL process   
`sql_queries.py`: script storing sql queries used in `create_tables.py` and `etl.py`  
`config.cfg`: config file to store some meta data  





