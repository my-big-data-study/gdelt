#!/usr/bin/env bash

export DOCKER_HOST_IP=$(ifconfig en0 | grep inet\[\ \] | awk \{print\ \$2\})

if [[ $# == 0 ]]; then
  docker-compose up -d
else
  docker-compose up -d $@
fi

for i in $*; do #在"$*"中遍历参数，此时"$*"被扩展为包含所有位置参数的单个字符串，只遍历一次
  case $i in
  airflow)
    echo "Airflow Web UI(8080): http://${DOCKER_HOST_IP}:8888"
    ;;
  kafka-manager)
    echo "Kafka-Manager Web UI: http://${DOCKER_HOST_IP}:9000"
    ;&
  kafka)
    echo "Kafka Rest URL: http://${DOCKER_HOST_IP}:9092"
    ;;
  kibana)
    echo "Kibana Web UI: http://${DOCKER_HOST_IP}:5601"
    ;&
  es)
    echo "Elasticsearch: http://${DOCKER_HOST_IP}:9200"
    ;;
  spark-master | spark-worker | spark-standalone)
    echo "Spark-master: http://${DOCKER_HOST_IP}:8080"
    ;;
  spark-notebook)
    echo "Spark-notebook: http://${DOCKER_HOST_IP}:9001"
    ;;
  namenode | datanode)
    echo "Namenode: http://${DOCKER_HOST_IP}:50070"
    echo "Datanode: http://${DOCKER_HOST_IP}:50075"
    ;;
    #  hug)
    #    echo "Hue (HDFS Filebrowser): http://${DOCKER_HOST_IP}:8088/home"
    #    ;;
  esac
done
