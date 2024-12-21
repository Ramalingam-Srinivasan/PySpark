import sys
from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .master("local[3]") \
            .appName("HelloSparkSql") \
            .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) !=2:
        logger.error("usage : Hello Spark <filename>")
        sys.exit(-1)

    surveyDf = spark \
                .read \
                .option("header","true") \
                .option("inferSchema","true") \
                .csv(sys.argv[1])

    surveyDf.createOrReplaceTempView("survey_tbl")
    count_df = spark.sql("select Country , count(1) as count from survey_tbl where Age<40 group by Country")
    count_df.show()
