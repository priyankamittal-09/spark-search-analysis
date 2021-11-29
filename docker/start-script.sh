#!/bin/sh
# Script for running spark-search-analysis project
cd  /spark/bin
./spark-submit \
  --master local \
  --driver-memory 4g \
  --class com.data.Main \
  --conf "spark.hadoop.fs.s3a.access.key=$1" \
  --conf "spark.hadoop.fs.s3a.secret.key=$2" \
  --files /app/external-reference.conf  \
  --driver-java-options "-Dconfig.file=/app/external-reference.conf" \
  --conf "spark.executor.extraJavaOptions =-Dconfig.file=/app/external-reference.conf" \
  /app/spark-search-analysis-assembly-0.1.jar