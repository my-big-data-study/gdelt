import sys
from argparse import ArgumentParser

from pyspark.sql import SparkSession


class GdeltJob:
    def __init__(self, **kwargs):
        self.export_source = kwargs.get('exportSource')
        self.source = kwargs.get('source')

    def operate_data(self, spark):
        data_frame = spark.read.option("header", "false") \
            .option("delimiter", "\t") \
            .option("inferSchema", "true") \
            .csv(self.source)

        return data_frame.toDF("GLOBALEVENTID",
                               "EventTimeDate",
                               "MentionTimeDate",
                               "MentionType",
                               "MentionSourceName",
                               "MentionIdentifier",
                               "SentenceID",
                               "Actor1CharOffset",
                               "Actor2CharOffset",
                               "ActionCharOffset",
                               "InRawText",
                               "Confidence",
                               "MentionDocLen",
                               "MentionDocTone",
                               "MentionDocTranslationInfo",
                               "Extras")


def load_args(argv):
    parser = ArgumentParser()
    parser.add_argument('--exportSource')
    parser.add_argument('--source')
    parser.add_argument('--esUrl')
    parser.add_argument('--esPort')
    parser.add_argument('--esUserName')
    parser.add_argument('--esUserPassword')

    args = parser.parse_args(argv[1:])
    return vars(args)


def write_to_es(df, **kwargs):
    df.write.format('org.elasticsearch.spark.sql') \
        .option('es.nodes', kwargs.get("esUrl")) \
        .option('es.port', kwargs.get("esPort")) \
        .option('es.nodes.wan.only', 'true') \
        .option("es.net.http.auth.user", kwargs.get("esUserName")) \
        .option("es.net.http.auth.pass", kwargs.get("esUserPassword")) \
        .option('es.mapping.id', 'GLOBALEVENTID') \
        .mode('append') \
        .save("gdelt/gdelt")


def main(argv):
    args_map = load_args(argv)
    spark_job = GdeltJob(**args_map)

    spark_session = SparkSession.builder.appName('gdelt_job') \
        .config('spark.jars.packages', 'org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2') \
        .getOrCreate()

    t_df = spark_job.operate_data(spark_session)

    t_df.cache()
    t_df.count()

    write_to_es(t_df, **args_map)


if __name__ == '__main__':
    main(sys.argv)
