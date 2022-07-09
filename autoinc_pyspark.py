#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('SparkMiniProject').getOrCreate()
sc = spark.sparkContext

raw_rdd = sc.textFile("data.csv")

def extract_vin_key_value(row):
    auto = row.strip().split(",")
    return (auto[2], (auto[1], auto[3], auto[5]))

vin_kv = raw_rdd.map(lambda row: extract_vin_key_value(row))

def populate_make(row):
    group_row = []
    for i in row:
        group_row.append(list(i))
    for j in range(0, len(group_row)):
        make = group_row[j-1][1]
        year = group_row[j-1][2]
        if group_row[j][0] != 'I':
            if group_row[j][1] == '' and group_row[j][2] == '':
                group_row[j][1] = make
                group_row[j][2] = year
    return group_row

enhance_make = vin_kv.groupByKey().flatMap(lambda row: populate_make(row[1]))

def extract_make_key_value(row):

    if row[0] == 'A':
        return (row[1], row[2]), 1
    else:
        return (row[1], row[2]), 0

make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

acc_make_year = make_kv.reduceByKey(lambda x, y: x + y)

file = open('output.txt', 'x')
for acc in acc_make_year.collect():
    file.write('{}-{},{}\n'.format(acc[0][0], acc[0][1], acc[1]))
file.close()

out = []
for acc in acc_make_year.collect():
    out.append('{}-{},{}'.format(acc[0][0], acc[0][1], acc[1]))
rdd_out = sc.parallelize(out)
rdd_out.saveAsTextFile('HDFS_text_results')
