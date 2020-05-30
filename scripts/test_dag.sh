#!/bin/sh

rm -rf /tmp/data/*
docker exec -it airflow rm -rf logs
docker exec -it airflow airflow delete_dag gdelt -y
docker exec -it airflow airflow backfill -s 2019-05-28 --reset_dagruns --verbose --local -y gdelt

ls /tmp/data

