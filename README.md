# Transform-Load---Sprint2
A program to run ETL for system information and station information automatically as 

1. Drop table 
2. Transform JSON files to CSV 
3. Enrich stations information data with system information. Note that the system information has only one record. We can simply use cross join to accomplish this task. 
4. Load CSV files into a Hive table.

We can achieve this using Hive

Hive has the ability of parsing JSON objects as well. The required library should be loaded into the cluster. We can upload our JSON files on HDFS, create external table, use get_json_object function to parse data and load them into our destination table.
