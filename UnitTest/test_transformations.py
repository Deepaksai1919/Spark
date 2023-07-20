from unittest import TestCase, TestSuite, TextTestRunner
import findspark
findspark.init()
from pyspark.sql import SparkSession
from library.transformations import read_survey_df, count_by_country


class TransformationTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.appName('PySpark Test').getOrCreate()

    def test_data_file_loading(self):
        sample_df = read_survey_df(self.spark, 'test_data/sample.csv')
        self.assertEqual(sample_df.count(), 7, 'Record count should be 7')
    
    def test_country_count(self):
        sample_df = read_survey_df(self.spark, 'test_data/sample.csv')
        count_list = count_by_country(sample_df).collect()

        count_dict = {}

        for row in count_list:
            count_dict[row['country']] = row['count']

        value_dict = {
            'India': 1,
            'China': 2,
            'Russia': 1
        }
        for country, count in count_dict.items():
            self.assertEqual(count, value_dict.get(country), f'Count for country {country} should be {value_dict.get(country)}')

def create_suite():
    suite = TestSuite()
    suite.addTest(TransformationTestCase('test_data_file_loading'))
    suite.addTest(TransformationTestCase('test_country_count'))
    return suite


runner = TextTestRunner()
runner.run(create_suite())