import numpy as np
from pyspark import SparkContext
sc =SparkContext()
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/cleaned_*.csv')
df_complaint=df.groupBy('Borough').count()
df_demo = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/demograph.csv')
df_complaint_demo = df_complaint.join(df_demo, df_complaint['Borough'] == df_demo['BOROUGH']).orderBy('count',ascending=False)
df_complaint_demo.select(col('Borough'),col('count'),col('Total Population'),col('Median Age')).show()

'''
+-------------+-------+----------------+----------+                             
|      Borough|  count|Total Population|Median Age|
+-------------+-------+----------------+----------+
|     BROOKLYN|4819634|       2,504,700|      34.1|
|       QUEENS|3698679|       2,230,722|      37.2|
|    MANHATTAN|3256604|       1,585,873|      36.4|
|        BRONX|2886539|       1,385,108|      32.8|
|STATEN ISLAND| 808380|         468,730|      38.4|
+-------------+-------+----------------+----------+
'''
