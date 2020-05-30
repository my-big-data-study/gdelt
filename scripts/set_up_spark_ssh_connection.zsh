#!/usr/bin/env sh

# 批量清除默认的连接配置(太多了)
default_connections=$(airflow connections -l | grep default\' | awk -F ' ' '{print $2}')

for default_conn_id in ${default_connections}; do
  # remove_head_quota
  remove_head_quota=${default_conn_id#\'}
  #  remove_tail_quota
  default_conn_id_trimed=${remove_head_quota%\'}
  airflow connections --delete --conn_id ${default_conn_id_trimed}
done

# 创建一个对于spark的ssh连接配置

spark_ssh_conn_id='spark.dc.ssh'
spark_ssh_conn_exist=$(airflow connections -l | grep ${spark_ssh_conn_id} | awk -F ' ' '{print $2}')
if [ ! ${spark_ssh_conn_exist} ]; then
  airflow connections --add --conn_id 'spark.dc.ssh' --conn_type 'ssh' --conn_host 'spark-standalone' --conn_login 'root' --conn_port 22 --conn_extra '{"key_file":"/usr/local/airflow/spark/ssh/id_rsa","timeout":"20","compress":"false","no_host_key_check":"true","allow_host_key_change":"true"}'
else
  echo "${spark_ssh_conn_id} already exist"
fi