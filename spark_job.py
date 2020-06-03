import datetime
import sys
from argparse import ArgumentParser

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, dayofmonth, concat, lit
from pyspark.sql.types import FloatType, StringType, DateType


class GdeltJob:
    def __init__(self, **kwargs):
        self.source = kwargs.get('source')

    def operate_data(self, spark):
        data_frame = spark.read.option("header", "false") \
            .option("delimiter", "\t") \
            .option("inferSchema", "true") \
            .csv(self.source)

        return data_frame.toDF("GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate", "Actor1Code",
                               "Actor1Name",
                               "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
                               "Actor1Religion1Code",
                               "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
                               "Actor2Code",
                               "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
                               "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code",
                               "Actor2Type3Code",
                               "IsRootEvent",
                               "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale",
                               "NumMentions",
                               "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
                               "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat",
                               "Actor1Geo_Long",
                               "Actor1Geo_FeatureID", "Actor2Geo_Type", "Actor2Geo_FullName",
                               "Actor2Geo_CountryCode",
                               "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long",
                               "Actor2Geo_FeatureID",
                               "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
                               "ActionGeo_ADM1Code",
                               "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
                               "DATEADDED",
                               "SOURCEURL")

    def transform(self, df):
        date_convert_func = udf(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'), DateType())
        iso_format_func = udf(lambda x: x.isoformat(), StringType())
        return df \
            .withColumn('eventId', col('GLOBALEVENTID')) \
            .withColumn('eventDay', date_convert_func(col('SQLDATE'))) \
            .withColumn('sourceUrl', col('SOURCEURL')) \
            .withColumn('latitude', col('Actor1Geo_Lat').cast(FloatType())) \
            .withColumn('longitude', col('Actor2Geo_Long').cast(FloatType())) \
            .dropDuplicates(['sourceUrl']) \
            .withColumn('timestamp', iso_format_func(col('eventDay'))) \
            .withColumn('day', dayofmonth(col('eventDay'))) \
            .withColumn('location', concat(col("latitude"), lit(","), col("longitude"))) \
            .select('eventId', 'timestamp', 'day', 'sourceUrl', 'location', 'latitude', 'longitude') \
            .cache()

    def run(self, spark):
        df = self.operate_data(spark)
        return self.transform(df)


def load_args(argv):
    parser = ArgumentParser()
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
        .option('es.mapping.id', 'eventId') \
        .mode('append') \
        .save("gdelt/_doc")


def main(argv):
    args_map = load_args(argv)
    spark_job = GdeltJob(**args_map)

    spark_session = SparkSession.builder.appName('gdelt_job') \
        .config('spark.jars.packages', 'org.elasticsearch:elasticsearch-spark-20_2.11:7.4.2') \
        .getOrCreate()

    t_df = spark_job.run(spark_session)

    t_df.cache()
    t_df.count()

    write_to_es(t_df, **args_map)


if __name__ == '__main__':
    main(sys.argv)
