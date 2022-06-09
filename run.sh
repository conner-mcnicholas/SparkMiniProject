#!/bin/sh
case "$PYSPARK_DRIVER_PYTHON" in  "jupyter") unset PYSPARK_DRIVER_PYTHON; esac
spark-submit autoinc_pyspark.py > execution_log.txt
echo '\nsingle file results printed to annual_acc.txt'
echo '\nresults saved as text file in HDFS format in HDFS_text_results folder'
