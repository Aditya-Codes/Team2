import numpy as np
from pyspark import SparkContext
sc =SparkContext()
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/cleaned_*.csv')

df_weather = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/central_park_weather.csv')
df_weather = df_weather.withColumn('date_formated',F.concat(F.col('DATE').substr(5,2), F.lit('/'), F.col('DATE').substr(7,2),F.lit('/'),F.col('DATE').substr(1,4)))
