import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/jzm218/new_311.csv')


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

df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()


#Replacing invalid Zipcodes with N/A
df = df.withColumn('Incident Zip', when(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!= True, 'N/A').otherwise(df['Incident Zip']))


# writing cleaned data onto new csv for comparision
df.write.format('com.databricks.spark.csv').options(header='true').save('/user/sdv267/cleaned_311.csv')                                                                
