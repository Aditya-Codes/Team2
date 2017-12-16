

#df = df.filter(df['Created Date'].like('%2009%')) # This is how you filter the date by year! 


'''
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

'''
