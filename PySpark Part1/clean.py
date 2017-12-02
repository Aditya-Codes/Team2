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
#Finding Unspecified Borough entries
df.where(col('Borough').isin("BRONX","BROOKLYN","MANHATTAN","STATEN ISLAND","QUEENS")!=True).groupBy('Borough').count().show()
'''
+-----------+------+
|    Borough| count|
+-----------+------+
|Unspecified|224127|
+-----------+------+
'''

