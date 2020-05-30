#!/usr/bin/env bash

APP_DIR="s3://data-learning-test/en/data"
ELASTIC_HADOOP_FILE_NAME="elasticsearch-hadoop-7.4.0.jar"
ELASTIC_PORT=443
ELASTIC_URL="https://search-data-training-zqyang-bngqxxpplpvhgq524vbxfnvfaa.us-east-2.es.amazonaws.com"
ELASTIC_USER_NAME="changeme"
ELASTIC_USER_PASSWORD="changeme"
EMR_SSH_CONNECTION="hadoop@ec2-18-217-22-20.us-east-2.compute.amazonaws.com"
EMR_SSH_KEY_PATH="~/data-training-zqyang.pem"
ELASTIC_JAR_ON_S3=$APP_DIR/$ELASTIC_HADOOP_FILE_NAME
ELASTIC_AUTH_HEADER="Authorization: Basic ennnnnOjQ4UHZUMkoyabcdeRW05Smo="

REMOTE_CSV_FILE_PATH=$1

SPARK_COMMAND=$(cat<<EOF
HADOOP_USER_NAME=hadoop spark-submit \
--jars $ELASTIC_JAR_ON_S3 \
--master yarn s3://data-learning-test/en/data/spark_job.py \
--exportSource='$REMOTE_CSV_FILE_PATH' \
--esUrl='$ELASTIC_URL' \
--esPort='$ELASTIC_PORT' \
--esUserName='$ELASTIC_USER_NAME' \
--esUserPassword='$ELASTIC_USER_PASSWORD'
EOF
)

aws --profile profile_tw s3 rm $APP_DIR/spark_job.py
aws --profile profile_tw s3 cp ./spark_job.py $APP_DIR/spark_job.py
#aws --profile profile_tw s3 cp ./data/elasticsearch-hadoop-7.4.0/dist/$ELASTIC_HADOOP_FILE_NAME $ELASTIC_JAR_ON_S3

echo "Begin to run $SPARK_COMMAND"

#curl -X "DELETE" -H "$ELASTIC_AUTH_HEADER" $ELASTIC_URL/testindex
## create the index manually
#curl -X PUT $ELASTIC_URL/testindex -H "$ELASTIC_AUTH_HEADER" -H 'Content-Type: application/json' -d'
#{
#    "mappings" : {
#        "properties" : {
#            "day" : { "type" : "integer" },
#            "doc_id" : { "type" : "text" },
#            "sourceUrl" : { "type" : "text" },
#            "timestamp" : { "type" : "date" },
#            "title" : { "type" : "text" },
#            "latitude" : { "type" : "float" },
#            "longitude" : { "type" : "float" },
#            "location" : { "type" : "geo_point" }
#        }
#    }
#}
#'

ssh -i $EMR_SSH_KEY_PATH $EMR_SSH_CONNECTION "$SPARK_COMMAND"

