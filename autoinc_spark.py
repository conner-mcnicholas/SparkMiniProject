import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext("local", "post-sales-auto")

raw_rdd = sc.textFile("data.csv")

vin_kv = raw_rdd.map(lambda x: (x.split(',')[2],[x.split(',')[3],x.split(',')[5],x.split(',')[1]]))

enhance_make = vin_kv.groupByKey().flatMap(lambda x: [(item[0],item[1]) for (item) in x[1]]).filter(lambda x:x[0]!='' and x[1]!='')

make_kv = enhance_make.map(lambda x: ((x[0],x[1]),1))

from operator import add
acc_make_year=make_kv.reduceByKey(add)

file = open('output.txt', 'x')
for acc in acc_make_year.collect():
    file.write('{}-{},{}\n'.format(acc[0][0], acc[0][1], acc[1]))
file.close()

out = []
for acc in acc_make_year.collect():
    out.append('{}-{},{}'.format(acc[0][0], acc[0][1], acc[1]))
rdd_out = sc.parallelize(out)
rdd_out.saveAsTextFile('HDFS_text_results')
