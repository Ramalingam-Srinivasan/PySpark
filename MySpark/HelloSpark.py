import sys
from pyspark.sql import *
from lib.logger import Log4j

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    logger = Log4j(spark)



    logger.info("Starting HelloSpark")

    logger.info("Finished HelloSpark")
    #spark.stop()
