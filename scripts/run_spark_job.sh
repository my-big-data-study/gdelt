#!/usr/bin/env bash

jenv local 1.8


SPARK_HOME="/Users/zqyang/WorkSpace/spark-2.4.5-bin-hadoop2.7"
APP_DIR="/Users/zqyang/WorkSpace/gdelt-project"
PY_FILES="$APP_DIR/gdelt_event.py"
SPARK_URL="spark://Zhengquan-Yangs-MacBook-Pro.local:7077"

$SPARK_HOME/bin/spark-submit --driver-memory=5G --executor-memory=10G --py-files=$PY_FILES --master $SPARK_URL $APP_DIR/spark_job.py --exportSource='/tmp/data/20200526040000.export.CSV' --mentionSource='/tmp/data/20200526040000.mentions.CSV' --esUrl='http://localhost:9200'
