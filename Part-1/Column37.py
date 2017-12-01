#!/usr/bin/env python

#script to clean the IncidentZip column (#37)


import sys
from pyspark import SparkContext
import csv
import re
from datetime import datetime

tests = [
   #(Type, Test)
    (int, int),
    (str,str),
    (float, float),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d"))
]

def getValidEntries(val):
    patt = re.compile('^\d{5}(?:[-\s]\d{4})?$')
    if (patt.match(str(val))):
        return True
    else:
        return False

def getDataType(key):
    label="invalid"
    for typ, test in tests:
        try:
            test(key)
            typ=str(typ).strip('<type').strip('>').strip(' ').strip('\'')
            myFlag = bool(getValidEntries(key))
            if (typ=="int" or typ=="str") and myFlag:
                label="valid"
            val=typ+","+"ZipCode,"+label

            return key,val
        except ValueError:
            continue
     #Not a match
    val="str,ZipCode,invalid"
    return key,val


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: column-37.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[36])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("column-37.txt")

    sc.stop()
