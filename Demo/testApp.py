from pyspark.sql import SparkSession
from pyspark.sql.functions import split,trim,col

spark = SparkSession.builder.appName('splitProblem') \
            .master('local[3]') \
            .getOrCreate()

data_path = spark.read.text('ip.txt')

df = data_path.select(split(trim(col('values')),',').alias('columns'))
df = df.select(col('columns')[0].cast('int').alias('ID'),
               col('columns')[1].alias('fname'),
               col('columns')[2].alias('lname'),
               split(col('columns')[3]),'|').getItem(0)
df.show()