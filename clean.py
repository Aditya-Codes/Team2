import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql.types import StringType
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan,udf

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

for x in df.schema.names:
	# Basic replacement 
	df = df.withColumn(x, when(col(x).isin("", "Unspecified", "0 Unspecified") != True, col(x)).otherwise('N/A'))  

# NA Statistics
df.select([count(when(col(c).isin('N/A'), c)).alias(c) for c in df.columns]).show()

df.write.format('com.databricks.spark.csv').options(header='true').save('cleaned_311.csv') 

'''
df.fillna(0)

#def validAgency(x):
#	return when(~ (col(x) != "3-1-1") | col(x).rlike('\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b') == True, #col(x)).otherwise(None)
	
new_column_udf = udf(lambda name: None if name == "" | (name != "3-1-1" & ~col(name).rlike('\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b')) else name, StringType())
	
df = df.withColumn("Agency", new_column_udf(df["Agency"])) # Cleaning agency column


df.cllect()

df.select(count(when(isnan("Agency") | col("Agency").isNull(), "Agency")).alias("Agency")).show()


'''

#Finding invalid Zip Codes
#df.where(length(col('Incident Zip')) > 0).select(col('Incident Zip')).filter(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!=True).groupBy('Incident Zip').count().show()

#df = df.withColumn('Incident Zip', when(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!= True, 'N/A').otherwise(df['Incident Zip']))

#Finding Unspecified Borough entries
#df.where(col('Borough').isin("BRONX","BROOKLYN","MANHATTAN","STATEN ISLAND","QUEENS")!=True).groupBy('Borough').count().show()


#Finding invalid Agency Acronyms.
#df.where(length(col('Agency')) > 0).select(col('Agency')).filter(col('Agency').rlike('\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b')!=True).groupBy('Agency').count().show()
# found that the invalid ones are 3-1-1 which is a valid agency acronym. Hence no outliers.

#Finding invalid Complaint Descriptors
#df.where(length(col('Descriptor')) > 0).select(col('Descriptor')).filter(col('Descriptor').rlike('^(?:[A-Z]|[a-z]|[0-9]|&|/|\s|\(|\)|,|\+|\.|"|-|\:)+$')!=True).groupBy('Descriptor').count().show()
#found no invalid entries
        
#Finding invalid 
#df.where(length(col('Community Board')) > 0).select(col('Community Board')).filter(col('Community Board').rlike("^(?:[A-Za-z0-9 ])+$")!=True).groupBy('Community Board').count().show()
#found no invalid entries

    
#df.write.format('com.databricks.spark.csv').save('cleaned_311.csv')                                           
