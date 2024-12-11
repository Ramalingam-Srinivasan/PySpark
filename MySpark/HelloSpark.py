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

   # trasformers and actions

    #trasnformations are lazy and actions are evaluated immediately

    #read, write, collect, show - > actions
    #where, select, filter, group by -> transformation

    survey_df = load_survey_df(spark,sys.argv[1])
    filtered_survey = survey_df.where("Age < 40").select("Age","Gender","Country","state")
    grouped_survey = filtered_survey.groupBy("Country")
    count_survey = grouped_survey.count()
    count_survey.show()
    logger.info("Finished HelloSpark")
    #spark.stop()
