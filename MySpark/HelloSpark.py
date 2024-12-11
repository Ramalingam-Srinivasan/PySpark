import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_survey_df

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession \
        .builder \
       .config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    logger = Log4j(spark)

    if len(sys.argv)!=2:
        logger.error("usage:hello spark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out)

    survey_df = load_survey_df(spark,sys.argv[1])
    survey_df.show()
    logger.info("Finished HelloSpark")
    #spark.stop()
