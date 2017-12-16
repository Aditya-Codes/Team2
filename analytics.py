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

# Freqency of complaints closing duration
from pyspark.sql import functions as F
df = df.withColumn('date_created', col('Created Date').substr(0,10))
df = df.withColumn('date_closed', col('Closed Date').substr(0,10))
timeFmt = "MM/dd/YYYY"
timeDiff = (F.unix_timestamp('date_closed', format=timeFmt) - F.unix_timestamp('date_created', format=timeFmt))
df = df.withColumn("Duration",timediff)
df = df.withColumn("DayDuration", df.Duration/86400)
df.groupBy('DayDuration').count().orderBy('count',ascending=False).show()


# Distribution of Complaints by time of day.
df = df.withColumn('hour_created',F.concat(F.col('Created Date').substr(12,2), F.lit(' '), F.col('Created Date').substr(21,2)))
#df.groupBy('hour_created').count().show()
df = df.withColumn('DayZone', when(col('hour_created').isin("5 AM","6 AM","7 AM","8 AM","9 AM","10 AM","11 AM")== True, 'morning')
df = df.withColumn('DayZone', when(col('hour_created').isin("12 PM","1 PM","2 PM","3 PM","4 PM","5 PM","6 PM","7 PM")== True, 'noon/evening').otherwise(df['DayZone']))
                   
df = df.withColumn('DayZone', when(col('hour_created').isin("8 PM","9 PM","10 PM","11 PM","12 AM","1 AM","2 AM","3 AM","4 AM")== True, 'night').otherwise(df['DayZone']))
df.groupBy('DayZone').count().show()            
                   

