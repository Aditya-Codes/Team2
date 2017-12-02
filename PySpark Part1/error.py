import numpy as np
from pyspark import SparkContext
sc =SparkContext()
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from pyspark.sql.functions import count,length,col
from pyspark.sql.types import LongType


df = sql_c.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/sdv267/new_311.csv')


#df.where(col('col1').like("%string%")).show()
#df.where(length(col('Incident Zip')) > 0).select(col('Incident Zip')).filter(col('Incident Zip').rlike('^\d{5}(?:[-\s]\d{4})?$')!=True).groupBy('Incident Zip').count().show()

#df.where(col('Borough').isin("BRONX","BROOKLYN","MANHATTAN","STATEN ISLAND","QUEENS")!=True).groupBy('Borough').count().show()

#df.where(length(col('Agency')) > 0).select(col('Agency')).filter(col('Agency').rlike('\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b')!=True).groupBy('Agency').count().show()

month_31days=[1,3,5,7,8,10,12]
month_30days=[4,6,9,11]
month_28days=[2]


def get_valid_year(year_input):
    year = int(year_input)
    if 1900 < year < 2020:
        return True

def get_valid_DayMonth(month_input, day_input):
    month = int(month_input)
    day = int(day_input)
    if month in month_28days:
        if 0 < day < 29:
            return True
        return False
    if month in month_30days:
        if 0 < day < 31:
            return True
        return False
    if month in month_31days:
        if 0 < day < 32:
            return True
        return False
    return False

def getValidDate(val):

    val = parse(val)
    val = val.strftime("%m/%d/%Y %I:%M:%S")
    #print(val)
    date,time = val.split(' ')

    m, d, y = date.split('/')
    if get_valid_DayMonth(m, d) and get_valid_year(y):
        return 1
    else:
        return 0

date_udf=udf(getValidDate, LongType())

df.where(col('Created Date').date_udf("Created Date")!=1).groupBy('Created Date').count().show()
