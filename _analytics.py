

#df = df.filter(df['Created Date'].like('%2009%')) # This is how you filter the date by year! 


'''
# This is how you group complaints. 
df = df.withColumn(x, when(col(x).like('%Noise%') != True, col(x)).otherwise('NOISE'))
df = df.withColumn(x, when(col(x).like('%Street%') != True, col(x)).otherwise('STREET'))
df = df.withColumn(x, when(col(x).like('%HEAT%') != True, col(x)).otherwise('HEAT'))
'''
