from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j
from pyspark.sql import *


if __name__ == "__main__":
    spark = SparkSession.builder \
                .master("local[3]") \
                .appName("DataSinkDemo") \
                .getOrCreate()

    logger = Log4j(spark)

    flighttimeparquetdf = spark.read \
            .format("parquet") \
            .load("datasource/flight*.parquet")

    logger.info("Num Partition before :" + str(flighttimeparquetdf.rdd.getNumPartitions()))
    flighttimeparquetdf.groupby(spark_partition_id()).count().show()

    partitioneddf = flighttimeparquetdf.repartition(5)

    logger.info("Num partition after:" + str(partitioneddf.rdd.getNumPartitions()))

    partitioneddf.groupby(spark_partition_id()).count().show()

    '''
         partitioneddf.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path","dataSink/avro/") \
        .save()
        
    '''

    flighttimeparquetdf.write \
       .format("json") \
       .mode("overwrite") \
       .option("path","dataSink/json/") \
        .option("maxRecordsPerFile",10000) \
       .partitionBy("OP_CARRIER","ORIGIN") \
       .save()

