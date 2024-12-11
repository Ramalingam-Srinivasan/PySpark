from pyspark import SparkConf
import configparser

def get_spark_app_configs():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark_conf")

    for(key,val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)
    return spark_conf