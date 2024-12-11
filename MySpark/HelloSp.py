from pyspark.sql import *

if __name__ == "__main__":
    print("hello world")

    spark = SparkSession.builder.appName("HelloSp").master("local[2]").getOrCreate()

    datalist = [("rama", 32), ("priya", 26), ("tinku", 30)]

    df = spark.createDataFrame(datalist).toDF("Name", "Age")
    df.show()
