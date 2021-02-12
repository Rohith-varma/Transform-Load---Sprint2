# Transform-Load---Sprint2
A program to run ETL for system information and station information automatically as 

1. Drop table 
2. Transform JSON files to CSV 
3. Enrich stations information data with system information. Note that the system information has only one record. We can simply use cross join to accomplish this task. 
4. Load CSV files into a Hive table.

We can achieve this using Hive

Hive has the ability of parsing JSON objects as well. The required library should be loaded into the cluster. We can upload our JSON files on HDFS, create external table, use get_json_object function to parse data and load them into our destination table.

To get online feed of Bixi visit https://api.nextbike.net/maps/gbfs/v1/nextbike_pp/gbfs.json

Below is the schema for Enriched Station Information

Field name Type
system_id String
timezone String
station_id Integer
name String
short_name String
lat Double
lon Double
capacity Integer
