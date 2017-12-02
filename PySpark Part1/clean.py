import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/new_311.csv')



#Finding invalid Zip Codes
df.where(length(col('Incident Zip')) > 0).select(col('Incident Zip')).filter(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!=True).groupBy('Incident Zip').count().show()
'''
+------------+-----+
|Incident Zip|count|
+------------+-----+
|     UNKNOWN|    1|
|          NA|   12|
|       113??|    1|
|         N/A|   31|
|      000000|    1|
|           ?|    1|
|           X|    1|
|   402901921|    1|
|        0000|    1|
|   070545020|    1|
|      198884|    1|
|         103|    1|
+------------+-----+
'''
#Replacing invalid Zipcodes with N/A
df = df.withColumn('Incident Zip', when(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!= True, ’N/A’).otherwise(df['Incident Zip']))

#Finding Unspecified Borough entries
df.where(col('Borough').isin("BRONX","BROOKLYN","MANHATTAN","STATEN ISLAND","QUEENS")!=True).groupBy('Borough').count().show()
'''
+-----------+------+
|    Borough| count|
+-----------+------+
|Unspecified|224127|
+-----------+------+
'''

#Finding invalid Agency Acronyms.
df.where(length(col('Agency')) > 0).select(col('Agency')).filter(col('Agency').rlike('\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b')!=True).groupBy('Agency').count().show()
# found that the invalid ones are 3-1-1 which is a valid agency acronym. Hence no outliers.

#Finding invalid Complaint Descriptors
df.where(length(col('Descriptor')) > 0).select(col('Descriptor')).filter(col('Descriptor').rlike('^(?:[A-Z]|[a-z]|[0-9]|&|/|\s|\(|\)|,|\+|\.|"|-|\:)+$')!=True).groupBy('Descriptor').count().show()
#found no invalid entries

#Finding invalid 
df.where(length(col('Community Board')) > 0).select(col('Community Board')).filter(col('Community Board').rlike("^(?:[A-Za-z0-9 ])+$")!=True).groupBy('Community Board').count().show()
#found no invalid entries


