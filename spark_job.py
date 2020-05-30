import datetime
import json
import re
import sys
from argparse import ArgumentParser

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, dayofmonth, concat, lit
from pyspark.sql.types import StructField, StructType, DateType, StringType, FloatType


class GdeltJob:
    def __init__(self, **kwargs):
        self.export_source = kwargs.get('exportSource')

    def read_data(self, spark):
        return spark.read.format("csv").option("delimiter", "\t").load(self.export_source)

    @staticmethod
    def get_title(row):
        try:
            res = requests.get(row[3], verify=False, timeout=5)
            if res.ok:
                searched_result = re.search('<\W*title\W*(.*)</title', res.text, re.IGNORECASE)
                if searched_result is not None:
                    return searched_result.group(1)
            else:
                return 'Invalid URL'
        except requests.exceptions.RequestException:
            return "Unknown due to http Error"

    def parse_partition_titles(self, iterator):
        for row in iterator:
            yield list((row['eventId'], self.get_title(row)))

    def transform(self, df, spark):
        date_convert_func = udf(lambda x: datetime.datetime.strptime(x, '%Y%m%d'), DateType())
        iso_format_func = udf(lambda x: x.isoformat(), StringType())

        raw_df = df \
            .withColumnRenamed(df.columns[0], 'eventId') \
            .withColumnRenamed(df.columns[1], 'eventDay') \
            .withColumnRenamed(df.columns[60], 'sourceUrl') \
            .withColumnRenamed(df.columns[56], 'latitude') \
            .withColumnRenamed(df.columns[57], 'longitude') \
            .select('eventId', 'eventDay', 'sourceUrl', 'latitude', 'longitude') \
            .cache()
        raw_df.count()

        transformed_df = raw_df \
            .withColumn('latitude', raw_df['latitude'].cast(FloatType())) \
            .withColumn('longitude', raw_df['longitude'].cast(FloatType())) \
            .dropDuplicates(['sourceUrl']) \
            .withColumn('eventDay', date_convert_func(col('eventDay'))) \
            .withColumn('timestamp', iso_format_func(col('eventDay'))) \
            .withColumn('day', dayofmonth(col('eventDay'))) \
            .withColumn('location', concat(col("latitude"), lit(","), col("longitude"))) \
            .select('eventId', 'timestamp', 'day', 'sourceUrl', 'location', 'latitude', 'longitude') \
            .cache()
        transformed_df.count()

        list_with_title = self.parse_title_by_url(transformed_df)
        return self.merge_data_frame(transformed_df, list_with_title, spark)

    @staticmethod
    def merge_data_frame(source, target_list, spark):
        schema = StructType([
            StructField('eventId', StringType(), False),
            StructField('title', StringType(), True),
        ])
        target = spark.createDataFrame(target_list, schema)
        return source.join(target, "eventId", "inner")

    def parse_title_by_url(self, df):
        return df.rdd \
            .mapPartitions(self.parse_partition_titles) \
            .collect()

    def run(self, spark):
        raw_df = self.read_data(spark)
        return self.transform(raw_df, spark)


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--exportSource')
    parser.add_argument('--esUrl')
    parser.add_argument('--esPort')
    parser.add_argument('--esUserName')
    parser.add_argument('--esUserPassword')

    args = parser.parse_args(argv[1:])
    return vars(args)


def format_row_for_es(row):
    return row['doc_id'], json.dumps(row.asDict())


def write_to_es(df, **kwargs):
    es_write_conf = {
        "es.nodes": kwargs.get("esUrl"),
        "es.port": kwargs.get("esPort"),
        "es.resource": 'testindex/_doc',
        "es.input.json": "yes",
        "es.mapping.id": "doc_id",
        "es.net.ssl": "true",
        "es.nodes.wan.only": "true",
        "es.net.http.auth.user": kwargs.get("esUserName"),
        "es.net.http.auth.pass": kwargs.get("esUserPassword")
    }
    rdd = df \
        .withColumnRenamed('eventId', 'doc_id') \
        .rdd.map(lambda x: format_row_for_es(x))

    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)


def main(argv):
    args_map = load_args(argv)
    spark_job = GdeltJob(**args_map)

    spark_session = SparkSession.builder.appName('gdelt_job').getOrCreate()
    t_df = spark_job.run(spark_session)

    t_df.cache()
    t_df.count()

    write_to_es(t_df, **args_map)


if __name__ == '__main__':
    main(sys.argv)
