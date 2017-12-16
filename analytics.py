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
# Number of complaints grouped by Months
df = df.withColumn('month_created', col('Created Date').substr(0,2))
df.groupBy('month_created').count().show()
'''
+-------------+-------+                                                         
|month_created|  count|
+-------------+-------+
|           01|1714672|
|           02|1515579|
|           03|1603257|
|           04|1305673|
|           05|1365630|
|           06|1406936|
|           07|1404212|
|           08|1356323|
|           09|1325166|
|           10|1426312|
|           11|1408483|
|           12|1212192|
+-------------+-------+
'''

# Number of complaints per Complaint Type
df_complaints=df.groupBy('Complaint Type').count().orderBy('count',ascending=False)
df_complaints.write.format('com.databricks.spark.csv').save('/user/sdv267/complaint.csv')

# Freqency of complaints closing
from pyspark.sql import functions as F
timeFmt = "MM/dd/YYYY HH:mm:ss"
timeDiff = (F.unix_timestamp('Closed Date', format=timeFmt) - F.unix_timestamp('Created Date', format=timeFmt))
df = df.withColumn("Duration", timeDiff)
df.groupBy('Duration').count().orderBy('count',ascending=False).show()
