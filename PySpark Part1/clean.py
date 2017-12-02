import numpy as np
from pyspark import SparkContext
sc =SparkContext()

from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/new_311.csv')

df.where(length(col('Incident Zip')) > 0).select(col('Incident Zip')).filter(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!=True).groupBy('Incident Zip').count().show()
