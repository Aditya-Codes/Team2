# Analytics tasks:

  1. Distribution of Complaints by Boroughs.
  2. Distribution of Complaints every year
  3. Monthly Trend of Complaints.
  4. Trend of Complaints closed each year.
  5. Number of Complaints per type.
  6. Grouping complaints of similar type and finidng distribution.
  7. Trends in time taken to close a complaint.
  8. Distribution of Complaints by time of day.(morning, noon and night)
  9. Distribution of Complaint Types by Borough.
  
# Running The Scripts

*1. Copy and move the scripts onto your HPC cluster.*
 
    scp <filename>.py dumbo:/home/<netid>/ [Local Terminal]

*2. Move the cleaned csv file onto HDFS from the dumbo.*
 
    scp <filename>.csv dumbo:/home/<netid>/ [Local Terminal]
    hfs -put <filename>.csv [HPC Cluster terminal]

*3. Change all the paths for input file and output file in analytics.py*
  
*4. Run the scripts:*
 
      pyspark --packages com.databricks:spark-csv_2.10:1.4.0 analytics.py
 
*5. Gather output csv's onto your local machine.*
 
    hfs -getmerge <output-filename>.csv <output-filename>.csv [HPC Cluster Terminal]
    scp dumbo:/home/<netid>/<output-filename>.csv <output-filename>.csv [Local Terminal]
