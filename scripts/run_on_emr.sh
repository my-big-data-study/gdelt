#!/usr/bin/env bash

APP_DIR="s3://data-learning-test/data"
ELASTIC_PORT=443
ELASTIC_URL="https://search-summer-x5ttza36jewgghjbq64nhuf7tm.ap-southeast-1.es.amazonaws.com"
ELASTIC_USER_NAME="admin"
ELASTIC_USER_PASSWORD="HQtmh101999."
EMR_SSH_CONNECTION="hadoop@ec2-13-229-62-2.ap-southeast-1.compute.amazonaws.com"
EMR_SSH_KEY_PATH="~/mhtang.pem"
ELASTIC_JAR="org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2"

REMOTE_CSV_FILE_PATH=$1

SPARK_COMMAND=$(cat<<EOF
HADOOP_USER_NAME=hadoop spark-submit \
--packages $ELASTIC_JAR \
--master yarn s3://data-learning-test/data/spark_job.py \
--exportSource='$REMOTE_CSV_FILE_PATH' \
--source='$REMOTE_CSV_FILE_PATH' \
--esUrl='$ELASTIC_URL' \
--esPort='$ELASTIC_PORT' \
--esUserName='$ELASTIC_USER_NAME' \
--esUserPassword='$ELASTIC_USER_PASSWORD'
EOF
)

aws --profile profile_tw s3 rm $APP_DIR/spark_job.py
aws --profile profile_tw s3 cp ./spark_job.py $APP_DIR/spark_job.py

echo "Begin to run $SPARK_COMMAND"

ssh -i $EMR_SSH_KEY_PATH $EMR_SSH_CONNECTION "$SPARK_COMMAND"

