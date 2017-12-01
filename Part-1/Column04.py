#!/usr/bin/env python

#script to clean the Agency column (#4)


import sys
from pyspark import SparkContext
import csv
import re
from datetime import datetime
import os

tests = [
   #(Type, Test)
    (int, int),
    (str,str),
    (float, float),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y"))
]

def getValidAgency(agency):
    patternmatch = re.compile("\\b[A-Z]{3}\\b|\\b[A-Z]{4}\\b|\\b[A-Z]{5}\\b")

    if patternmatch.match(agency):
        return True
    else:
        return False

def getDataType(key):
    label="invalid"
    for typ, test in tests:
        try:
            test(key)
            typ=str(typ).strip('<type').strip('>').strip(' ').strip('\'')
            myFlag = bool(getValidAgency(key))
            if (typ=="str") and myFlag:
                label="valid"
            val=typ+","+"Agency,"+label

            return key,val
        except ValueError:
            continue
     #Not a match
    val="str,Agency,invalid"
    return key,val


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: column-04.py <file>")
        exit(-1)
    sc = SparkContext()
    csvfile = sc.textFile(sys.argv[1],1)
    unique_key = csvfile.mapPartitions(lambda x: csv.reader(x)).map(lambda x: x[3])
    base_type=unique_key.map(lambda x: getDataType(x))
    base_type.saveAsTextFile("column-04.txt")
    os.system("hfs -getmerge column-04.txt column-04.txt")
    sc.stop()
