from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_survey_df,count_by_country

class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession \
                    .builder \
                    .appName("HelloSparkTest") \
                    .master("local[3]") \
                    .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_survey_df(self.spark,"C:/Users/ram14/PycharmProjects/spark/PySpark/MySpark/data/sample.csv")
        count_df = sample_df.count()
        self.assertEqual(count_df,9,"record count should be 9")

    def test_country_by_count(self):
        sample_df = load_survey_df(self.spark,"C:/Users/ram14/PycharmProjects/spark/PySpark/MySpark/data/sample.csv")
        count_list = count_by_country(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["Country"]] = row["count"]
        self.assertEqual(count_dict["United States"],4,"usa should have 4 records")
        self.assertEqual(count_dict["Canada"],2,"canada should have 2 recods")


   # @classmethod
    #def tearDownClass(cls)->None:
     #   cls.spark.stop()