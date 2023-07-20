import sys
import findspark
findspark.init()
from pyspark.sql import SparkSession
from library.transformations import read_survey_df, count_by_country

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: pyspark-test <fileName>')
        sys.exit(-1)
    data_file = sys.argv[1]
    spark = SparkSession.builder.appName('PySpark Test').getOrCreate()
    print('Processing test file:', data_file)
    survey_df = read_survey_df(spark, data_file)
    count_df = count_by_country(survey_df)
    count_df.show()
    print('Finished processing:', data_file)
