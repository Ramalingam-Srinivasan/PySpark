from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
    .master("local[3]") \
    .appName("SparkSchemaDemo") \
    .getOrCreate()

    logger = Log4j(spark)

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
            ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
            WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flightTimecsvData = spark.read \
                .format("csv") \
                .schema(flightSchemaStruct) \
                .option("header","true") \
                .option("mode","failFast") \
                .option("dateFormat","M/d/y") \
                .load("data/flight*.csv")

    flightTimecsvData.show(5)

    logger.info("csv schema :" + flightTimecsvData.schema.simpleString())

    flightTimejsonData = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat","M/d/y") \
        .load("data/flight*.json")

    flightTimejsonData.show(5)

    logger.info("json schema :"+flightTimejsonData.schema.simpleString())

    flightTimeparquetData = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flightTimeparquetData.show(5)

    logger.info("parquet data:"+flightTimeparquetData.schema.simpleString())