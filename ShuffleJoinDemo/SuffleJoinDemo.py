from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("ShuffleJoinDemo") \
            .master("local[3]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()

    logger = Log4j(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")


    #since join operations are transformation process untill action performed nothing will happen ,
    #hence using dummy foreach loop to create a action process
    join_df.foreach(lambda  x :  None)

    input("press any key to stop")
