# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 19:38:37 2019

@author: srjalan
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrdersByCustomerSorted")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    orders = float(fields[2])
    return (customer, orders)

lines = sc.textFile("data/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByCustomer = rdd.reduceByKey(lambda x, y: (x + y))
totalsByCustomerSorted = totalsByCustomer.map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0]))



results = totalsByCustomerSorted.collect()
for result in results:
    print(result)
