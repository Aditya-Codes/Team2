import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql.types import StringType
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan,udf

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/jzm218/new_311.csv')

# dropping columns which are not necessary and donot contain much information as majority entries were null.
drop_list = ['Landmark',
			'Facility Type', 
			'School Name', 
			'School Number', 
			'School Region', 
			'School Code', 
			'School Phone Number', 
			'School Address', 
			'School City', 
			'School State', 
			'School Zip',
			'School Not Found',
			'School or City Wide Complaint',
			'Bridge Highway Name',
			'Bridge Highway Direction',
			'Road Ramp',
			'Bridge Highway Segment',
			'Garage Lot Name',
			'Ferry Direction',
			'Ferry Terminal Name',
			'Latitude',
			'Longitude']

df = df.select([name for name in df.schema.names if name not in drop_list])

#Replacing invalid Zipcodes with N/A Zip codes should either be 5 digits or 5 digits followed by 4 digits.
df = df.withColumn('Incident Zip', when(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!= True, 'N/A').otherwise(df['Incident Zip']))

# Replacing invalid closed dates (before 2009) with N/A
years = [str(i) for i in range(2009, 2018)]
df = df.withColumn('Closed Date',when( col('Closed Date').substr(7,4).isin(years), col('Closed Date')).otherwise('N/A'))

#Replacing Null values with "N/A"
for x in df.schema.names:
	# Basic replacement 
	df = df.withColumn(x, when(col(x).isin("", "Unspecified", "0 Unspecified") != True, col(x)).otherwise('N/A'))  


#writing cleaned dataframes onto a csv for comparision and Analysis phase.
df.write.format('com.databricks.spark.csv').options(header='true').save('/user/sdv267/cleaned_311.csv') 


                                         
