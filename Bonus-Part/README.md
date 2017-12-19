# Hypothesis:

  1. The number of complaints increase with decrease in temperature.
  2. The number of complaints increase with increase in total population in Boroughs.
  3. The number of complaints increase with increase in distribution of population by Zipcode.
  
# Running The Scripts

*1. Copy and move the scripts onto your HPC cluster.*
 
    scp <filename>.py dumbo:/home/<netid>/ [Local Terminal]

*2. Move the additional weather and demograph and zipcode_pop datasets csv file onto HDFS from the dumbo.*
 
    scp <filename>.csv dumbo:/home/<netid>/ [Local Terminal]
    hfs -put <filename>.csv [HPC Cluster terminal]

*3. Change all the paths for input file and output file in both python scripts*
  
*4. Run the respective scripts:*
 
      pyspark --packages com.databricks:spark-csv_2.10:1.4.0 <filename>.py
  
*5. Gather output csv's onto your local machine.*
 
    hfs -getmerge <output-filename>.csv <output-filename>.csv [HPC Cluster Terminal]
    scp dumbo:/home/<netid>/<output-filename>.csv <output-filename>.csv [Local Terminal]
    
*6. Run the pearson_coeff.py script to find the Pearson Co-relation factor to determine the corelatibility of the hypothesis*
    
    python pearson_coeff.py
