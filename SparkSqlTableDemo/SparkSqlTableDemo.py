from pyspark.sql import *
from lib.logger import Log4j


if __name__ == "__main__":
    # Create a SparkSession with Hive support enabled
    spark = SparkSession \
                .builder \
                .master("local[3]") \
                .appName("SparkSqlTableDemo") \
                .enableHiveSupport() \
                .getOrCreate()

    # Initialize the logger
    logger = Log4j(spark)

    # Read a Parquet file from the specified directory
    flightgTimeParquetDF = spark.read \
                    .format("parquet") \
                    .load("dataSource/")

    # Create a new database if it doesn't exist and set it as the current database
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # Write the DataFrame to a Hive table in CSV format with bucketing and sorting
    #if we use partition by it will create huge paritions which includes duplicate values as well , hence used bucket by

    flightgTimeParquetDF.write \
    .format("csv") \
    .mode("overwrite") \
    .bucketBy(5, "OP_CARRIER", "ORIGIN") \
    .sortBy("OP_CARRIER", "ORIGIN") \
    .saveAsTable("flight_time_tbl")
