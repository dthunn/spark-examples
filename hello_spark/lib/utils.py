import configparser
from pyspark import SparkConf


def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/" + data_file)


def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.RawConfigParser()
    config.optionxform = str
    config.read("spark.conf")
    for k, v in config.items("SPARK_APP_CONFIGS"):
        print(f"k={k}, v={v}")
        spark_conf.set(k, v)
    return spark_conf