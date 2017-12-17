import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/cleaned_*.csv')

# Number of complaints per Borough
df.groupBy('Borough').count().show()
'''
+-------------+-------+                                                         
|      Borough|  count|
+-------------+-------+
|          N/A|1574599|
|    MANHATTAN|3256604|
|       QUEENS|3698679|
|STATEN ISLAND| 808380|
|     BROOKLYN|4819634|
|        BRONX|2886539|
+-------------+-------+
'''

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

# Number of complaints closed per year
df = df.withColumn('year_closed', col('Closed Date').substr(7,4))
df.groupBy('year_closed').count().orderBy('count',ascending=False).show()
'''
+-----------+-------+                                                           
|year_closed|  count|
+-----------+-------+
|       2016|2303123|
|       2015|2243518|
|       2017|2193101|
|       2014|2057229|
|       2010|1856035|
|       2013|1799583|
|       2011|1792802|
|       2012|1730636|
|           | 584559|
|       2009| 477384|
|       1900|   6201|
|       2008|    235|
|       2001|      8|
|       2000|      4|
|       2201|      3|
|       2019|      3|
|       2006|      3|
|       3009|      1|
|       1939|      1|
+-----------+-------+
'''
# Number of complaints per Complaint Type
df_complaints=df.groupBy('Complaint Type').count().orderBy('count',ascending=False)
df_complaints.write.format('com.databricks.spark.csv').save('/user/sdv267/complaint.csv')

# Grouping Like Complaint types
x= 'Complaint Type'
# Noise Group
df = df.withColumn(x, when(col(x).like('%Noise%') != True, col(x)).otherwise('NOISE'))
# Street Group
df = df.withColumn(x, when(col(x).like('%Street%') != True, col(x)).otherwise('STREET'))
# Heat Group
df = df.withColumn(x, when(col(x).isin('HEAT/HOT WATER','HEATING', 'Non-Residential Heat', 'Sidewalk Cafe Heater') != True, col(x)).otherwise('HEAT'))
# Water group
df = df.withColumn(x, when(col(x).like('%Water%') != True, col(x)).otherwise('WATER'))
# Construction Group
df = df.withColumn(x, when(col(x).isin('GENERAL CONSTRUCTION', 'General Construction/Plumbing', 'Construction', 'CONSTRUCTION') != True, col(x)).otherwise('CONSTRUCTION'))
# Plumbing Group
df = df.withColumn(x, when(col(x).isin('Plumbing', 'PLUMBING') != True, col(x)).otherwise('PLUMBLING'))
# Paint Group
df = df.withColumn(x, when(col(x).isin('PAINT - PLASTER', 'PAINT/PLASTER') != True, col(x)).otherwise('PAINT'))

df_complaints=df.groupBy('Complaint Type').count().orderBy('count',ascending=False)
df_complaints.write.format('com.databricks.spark.csv').save('/user/sdv267/complaint_grouped.csv')
df.groupBy('Complaint Type').count().orderBy('count',ascending=False).show(10)
'''
+--------------------+-------+                                                  
|      Complaint Type|  count|
+--------------------+-------+
|               NOISE|2476943|
|                HEAT|1754711|
|              STREET|1695323|
|        CONSTRUCTION| 819432|
|           PLUMBLING| 678184|
|    Blocked Driveway| 645953|
|               PAINT| 629845|
|               WATER| 581095|
|     Illegal Parking| 542980|
|Traffic Signal Co...| 366360|
+--------------------+-------+
'''


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
df = df.withColumn('DayZone', when(col('hour_created').isin("05 AM","06 AM","07 AM","08 AM","09 AM","10 AM","11 AM")== True, 'morning'))
df = df.withColumn('DayZone', when(col('hour_created').isin("12 PM","01 PM","02 PM","03 PM","04 PM","05 PM","06 PM","07 PM")== True, 'noon/evening').otherwise(df['DayZone']))
df = df.withColumn('DayZone', when(col('hour_created').isin("08 PM","09 PM","10 PM","11 PM","12 AM","01 AM","02 AM","03 AM","04 AM")== True, 'night').otherwise(df['DayZone']))
df.groupBy('DayZone').count().show()        
'''
+------------+-------+                                                          
|     DayZone|  count|
+------------+-------+
|       night|7135692|
|     morning|4063862|
|        null|      2|
|noon/evening|5844881|
+------------+-------+
'''
                   
