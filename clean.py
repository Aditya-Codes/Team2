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


#Replacing Null values with "N/A"
for x in df.schema.names:
	# Basic replacement 
	df = df.withColumn(x, when(col(x).isin("", "Unspecified", "0 Unspecified") != True, col(x)).otherwise('N/A'))  

# NA Statistics
df.select([count(when(col(c).isin('N/A'), c)).alias(c) for c in df.columns]).show()

df.write.format('com.databricks.spark.csv').options(header='true').save('/user/sdv267/cleaned_311.csv') 


                                         
