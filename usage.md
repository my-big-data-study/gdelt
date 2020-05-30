
### 环境准备：

1. 最好为自己的Docker增加可分配的资源配置(cpu > 4G+, memery > 4G+)


### 运行

在项目目录下运行 `. ./start_workbench.sh [airflow | kafka | spark-standalone | es | kibana]` 就可以启动 docker-compose.yml 中定义的单个或多个容器，不指定Service会默认启动所有服务

```bash
# 启动所有服务
. ./start_workbench.sh
# 只启动 airflow 和 kafka
. ./start_workbench.sh airflow kafka
```