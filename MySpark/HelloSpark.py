import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession \
        .builder \
       .config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    logger = Log4j(spark)

    logger.info("Starting HelloSpark")
    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out)
    logger.info("Finished HelloSpark")
    #spark.stop()
