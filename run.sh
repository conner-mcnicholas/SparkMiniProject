#!/bin/sh
python autoinc_pyspark.py > execution_log.txt
echo '\nsingle file results printed to annual_acc.txt'
echo '\nresults saved as text file in HDFS format in HDFS_text_results folder'
