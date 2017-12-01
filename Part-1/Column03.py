#!/usr/bin/env python

#script to clean ClosedDate(#3)

import sys
from pyspark import SparkContext
import csv
import re
from datetime import datetime
from dateutil.parser import parse


tests = [
   #(Type, Test)


    (int, int),
    (float, float),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y %I:%M:%S")),
    (str,str)
]
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

def getValidDate(val):

    val = parse(val)
    val = val.strftime("%m/%d/%Y %I:%M:%S")
    print(val)
    date,time = val.split(' ')

    m, d, y = date.split('/')
    if get_valid_DayMonth(m, d) and get_valid_year(y):
        return True
    else:
        return False



def getDataType(key):
        label = "invalid"
        for typ, test in tests:
                try:
                        test(key)
                        if typ == str:
                                if key == '':
                                        label = "N/A"
                                else:
                                        if getValidDate(key):
                                                label = "Valid"
                                                break
                                        label = "invalid"
                                        break
                        if typ == int:
                                break
                        if typ == float:
                                break
                        else:
                                break

                except ValueError:
                        continue

        return str(str(key)+', '+str(typ).replace('<class', '').strip('>')+', '+'Closed Date, '+label)




if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: column-03.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[2])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("column-03.txt")
    sc.stop()
