# Cleaning tasks:
  1. Drop columns with very few entries or too many null entries.
  2. Replace invalid entries with "N/A"
  3. Replace or null valued entries with "N/A"
  
# Statistics and Surprising Events:
  1. Find total number of null entries in each column.
  2. Find invalid entries based on regex pattern matching in columns.
  3. Find surprisingly low frequency entries to find any odd suspicious entries.
  4. Find surprisingly high frequency entries.
  
# RUNNING THE SCRIPTS

*1. Copy and move the scripts onto your HPC cluster.*
 
    scp <filename>.py dumbo:/home/<netid>/ [Local Terminal]

*2. Move the csv file onto HDFS from the dumbo.*
 
    scp <filename>.csv dumbo:/home/<netid>/ [Local Terminal]
    hfs -put <filename>.csv [HPC Cluster terminal]

*3. Change the paths for input file and output file in both clean.py and stats_surprises.py*
  
*4. Run the scripts:*
 
      Cleaning Script - pyspark --packages com.databricks:spark-csv_2.10:1.4.0 clean.py
      
      Statistics & Surprises - pyspark --packages com.databricks:spark-csv_2.10:1.4.0 stats_surprises.py
 
*5. Gather cleaned csv onto your local machine.*
 
    hfs -getmerge <output-filename>.csv <output-filename>.csv [HPC Cluster Terminal]
    scp dumbo:/home/<netid>/<output-filename>.csv <output-filename>.csv [Local Terminal]
