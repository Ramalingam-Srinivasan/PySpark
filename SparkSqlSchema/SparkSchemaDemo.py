from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
    .master("local[3]") \
    .appName("SparkSchemaDemo") \
    .getOrCreate()

    logger = Log4j(spark)

    flightTimecsvData = spark.read \
                .format("csv") \
                .option("header","true") \
                .option("inferSchema","true") \
                .load("data/flight*.csv")

    flightTimecsvData.show(5)

    logger.info("csv schema :" + flightTimecsvData.schema.simpleString())

    flightTimejsonData = spark.read \
        .format("json") \
        .load("data/flight*.json")

    flightTimejsonData.show(5)

    logger.info("json schema :"+flightTimejsonData.schema.simpleString())

    flightTimeparquetData = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flightTimeparquetData.show(5)

    logger.info("parquet data:"+flightTimeparquetData.schema.simpleString())