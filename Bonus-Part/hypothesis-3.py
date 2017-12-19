import numpy as np
from pyspark import SparkContext
sc =SparkContext()
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col,when,isnan

df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/cleaned_*.csv')
df_complaint=df.groupBy('Incident Zip').count()

df_demo = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/zipcode_pop.csv')
df_complaint_demo = df_complaint.join(df_demo, df_complaint['Incident Zip'] == df_demo['Zip Code']).orderBy('count',ascending=False)
df_hyp03=df_complaint_demo.select(col('Incident Zip'),col('count'),col('Population'))
df_hyp03.write.format('com.databricks.spark.csv').save('/user/sdv267/hypo3.csv')
