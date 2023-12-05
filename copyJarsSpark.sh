#!/usr/bin/env bash
path_origin="/Users/mangeluz/Documents/pythoncode/taxiDriver/venv/lib/python3.8/site-packages/pyspark/jars"
path_target="/Users/mangeluz/airflow/venv/lib/python3.8/site-packages/pyspark/jars"
list_jars=("aws-java-sdk-1.12.535.jar"
"aws-java-sdk-core-1.12.469.jar"
"aws-java-sdk-s3-1.12.469.jar"
"aws-java-sdk-1.7.4.jar"
"aws-java-sdk-dynamodb-1.12.469.jar"
"hadoop-aws-3.3.4.jar"
"spark-sql-kafka-0-10_2.12-3.4.1.jar"
"commons-pool2-2.11.0.jar"
"kafka-clients-3.3.2.jar"
"spark-token-provider-kafka-0-10_2.12-3.4.1.jar")

# shellcheck disable=SC1073
for jar in ${list_jars}; do
  cp ${path_origin}/${jar} ${path_target}
done

for x in ${list_jars}; do
  ls -l ${path_target}/${x}
done