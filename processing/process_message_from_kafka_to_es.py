from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StructField, IntegerType, DoubleType, StringType, StructType, FloatType
import sys


def setLogger(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("kafka").setLevel(logger.Level.ERROR)
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


class Gdelt:
    mapTypes = {
        "STRING": StringType(),
        "INTEGER": IntegerType(),
        "FLOAT": FloatType()
    }
    events_fields_name = ["GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate", "Actor1Code", "Actor1Name",
                          "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode", "Actor1Religion1Code",
                          "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code", "Actor2Code",
                          "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
                          "Actor2Religion1Code",
                          "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code", "IsRootEvent",
                          "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale", "NumMentions",
                          "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
                          "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat",
                          "Actor1Geo_Long",
                          "Actor1Geo_FeatureID", "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
                          "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long",
                          "Actor2Geo_FeatureID",
                          "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
                          "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID", "DATEADDED",
                          "SOURCEURL"]

    @staticmethod
    def get_event_schema():
        return StructType(list(map(lambda x: StructField(x, StringType()), Gdelt.events_fields_name)))

def array_column_to_csv(df, colName, csv_schema, sep="\t"):
    """
    将DateFrame中的一列分割，并且转换成CSV格式
    :param df: 源DataFrame
    :param colName: 需要分割转换的列名
    :param csv_schema: 转换后的视图
    :param sep: 分割符号
    :return: 转换后的DataFrame
    """
    df_columns_list = df.columns
    temp_array = colName + "array"

    df = df.withColumn(temp_array, split(df[colName], sep))
    return df.select(
        df_columns_list + [
            (col(temp_array)[x]).cast(csv_schema.fields[x].dataType.typeName()).alias(csv_schema.fields[x].name) for x
            in range(0, len(csv_schema.fields))]).drop(temp_array)


if __name__ == '__main__':
    kafka_host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    es_host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'

    print("kafka_host:", kafka_host, "es_host:", es_host)

    spark = SparkSession.builder.appName(name="kafka to es test") \
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.elasticsearch:elasticsearch-spark-20_2.11:7.6.0') \
        .master('local') \
        .getOrCreate()

    setLogger(spark)

    kafkaStreamDF = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_host + ":9092") \
        .option("subscribe", "gdelk_export") \
        .option("startingOffsets", "earliest") \
        .load()

    # kafkaStream.printSchema()
    # root
    # | -- key: binary(nullable=true)
    # | -- value: binary(nullable=true)
    # | -- topic: string(nullable=true)
    # | -- partition: integer(nullable=true)
    # | -- offset: long(nullable=true)
    # | -- timestamp: timestamp(nullable=true)
    # | -- timestampType: integer(nullable=true)

    kafkaStreamDF.isStreaming

    exprDf_0 = kafkaStreamDF.selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)", "timestamp")

    df_with_column = array_column_to_csv(exprDf_0, 'value', Gdelt.get_event_schema())

    df_with_column.printSchema()

    df_with_column \
        .writeStream \
        .outputMode('Append') \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_host) \
        .option("es.resource", "gdelk_export") \
        .option("es.nodes.wan.only", "true") \
        .option("es.nodes.discovery", "false") \
        .option("es.spark.sql.streaming.sink.log.enabled", "false") \
        .option("es.mapping.id", "GLOBALEVENTID") \
        .option("checkpointLocation", "/tmp/data/es") \
        .trigger(once=True) \
        .start() \
        .awaitTermination()
