import logging
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)
import pyspark.sql.functions as psf

from conf import (BROKER_URL, TOPIC_NAME, RADIO_CODE_FILE)

# TODO Create a schema for incoming resources
# NOTE: sample:
# {"crime_id": "183653737", "original_crime_type_name": "Traffic Stop", "report_date": "2018-12-31T00:00:00.000",
#  "call_date": "2018-12-31T00:00:00.000", "offense_date": "2018-12-31T00:00:00.000", "call_time": "23:46",
#  "call_date_time": "2018-12-31T23:46:00.000", "disposition": "CIT", "address": "Sansome St/chestnut St",
#  "city": "San Francisco", "state": "CA", "agency_id": "1", "address_type": "Intersection", "common_location": ""}
schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', StringType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = (
        spark.readStream
            .format('kafka')
            .option("kafka.bootstrap.servers", BROKER_URL)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 200)
            .option("stopGracefullyOnShutdown", "true")
            .load()
    )

    # Show schema for the incoming resources for checks
    logger.info("...Show schema for the df resource for check")
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = (
        kafka_df
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))
        .select("DF.*"))

    logger.info("...Show schema for the service_table resource for check")
    service_table.printSchema()

    # TODO select original_crime_type_name and disposition
    distinct_table =service_table.select(
                psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
                psf.col('original_crime_type_name'),
                psf.col('disposition')
    )

    logger.info("...Show schema for the distinct_table resource for check")
    distinct_table.printSchema()
    # return


    # NOTE: Aggregator/Join logic should show different types of crimes occurred in certain time frames
    # (using window functions, group by original_crime_time), and how many calls occurred in certain
    # time frames (group by on certain time frame, but the student will need to create a user defined
    # function internally for Spark to change time format first to do this operation). Any aggregator
    # or join logic should be performed with Watermark API. The students can play around with the time
    # frame - this is part of their analytical exercises and data exploration.


    # count the number of original crime type
    # NOTE: Tip (https://knowledge.udacity.com/questions/63483)
    # "you are suppose to find the count for every 60 minutes..."
    agg_df = (
        distinct_table
        .withWatermark('call_date_time', '60 minutes')
            .groupby(
                [psf.window('call_date_time', '15 minutes'),
                 'disposition',
                 'original_crime_type_name']
            )
            .count()
    )

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = (
        agg_df.writeStream
            .outputMode('Complete')
            .format('console')
            .start()
    )


    # TODO attach a ProgressReporter
    # query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = RADIO_CODE_FILE
    radio_code_df = spark.read.option("multiline",True).json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    logger.info("...Show schema for the radio_code_df resource for check")
    radio_code_df.printSchema()

    # TODO join on disposition column
    join_query = (
        agg_df.join(radio_code_df, 'disposition')
            .writeStream
            .outputMode('Complete')
            .format("console")
            .start()
    )

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = (
        SparkSession.builder
            .config("spark.streaming.kafka.maxRatePerPartition", 50)
            .config("spark.streaming.kafka.processedRowsPerSecond", 50)
            .config("spark.streaming.backpressure.enabled", "true")
            .master("local[*]")
            .appName("KafkaSparkStructuredStreaming")
            .getOrCreate()
            )
    # .config("spark.default.parallelism", 60)
    # .config("spark.sql.shuffle.partitions", 100)

    # NOTE: Comment that to see all the logs
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

