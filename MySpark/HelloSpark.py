import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_survey_df,count_by_country

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

   # trasformers and actions

    #trasnformations are lazy and actions are evaluated immediately

    #read, write, collect, show - > actions
    #where, select, filter, group by -> transformation

    survey_df = load_survey_df(spark,sys.argv[1])
    partitioned_survey = survey_df.repartition(2)
    count_survey = count_by_country(partitioned_survey)
   # count_survey.show()
    #collect will diaplay list of rows
    logger.info(count_survey.collect())
    input("press enter")
    logger.info("Finished HelloSpark")
    #spark.stop()
