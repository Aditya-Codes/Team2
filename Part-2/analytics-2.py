import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/cleaned_*.csv')

# Number of complaints per Borough
df.groupBy('Borough').count().show()

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

#df_complaints=df.groupBy('Complaint Type').count().orderBy('count',ascending=False)
#df_complaints.write.format('com.databricks.spark.csv').save('/user/sdv267/complaint_grouped.csv')
df.groupBy('Complaint Type').count().orderBy('count',ascending=False).show(10)

x = "Borough"
      
df.filter(col(x).isin("MANHATTAN")).groupBy('Complaint Type').count().orderBy('count',ascending=False).show()
df.filter(col(x).isin("BROOKLYN")).groupBy('Complaint Type').count().orderBy('count',ascending=False).show()
df.filter(col(x).isin("BRONX")).groupBy('Complaint Type').count().orderBy('count',ascending=False).show()
df.filter(col(x).isin("STATEN ISLAND")).groupBy('Complaint Type').count().orderBy('count',ascending=False).show()
df.filter(col(x).isin("QUEENS")).groupBy('Complaint Type').count().orderBy('count',ascending=False).show()
	





