import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/cleaned_*.csv')

# Number of complaints per Borough
df.groupBy('Borough').count().show()

# Number of complaints per year
df = df.withColumn('year', col('Created Date').substr(7,4))
df.groupBy('year').count().show()
'''
+----+-------+                                                                  
|year|  count|
+----+-------+
|2009| 489607|
|2010|2005760|
|2011|1918896|
|2012|1783212|
|2013|1849019|
|2014|2102226|
|2015|2286951|
|2016|2370339|
|2017|2238425|
+----+-------+
'''

# Number of complaints per Complaint Type
df_complaints=df.groupBy('Complaint Type').count().orderBy('count',ascending=False)
df_complaints.write.format('com.databricks.spark.csv').save('/user/sdv267/complaint.csv')
