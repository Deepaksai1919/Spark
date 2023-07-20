def read_survey_df(spark, data_file):
    return spark.read.csv(path = data_file, header = True, inferSchema = True)
def count_by_country(survey_df):
    return survey_df.filter('age > 40').groupBy('country').count()