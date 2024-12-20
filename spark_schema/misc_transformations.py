from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, when, expr
from pyspark.sql.types import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Misc Demo") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-rdd") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

    # df2 = df1.withColumn("year", expr("""
    #      case when year < 21 then year + 2000
    #      when year < 100 then year + 1900
    #      else year
    #      end"""))
    # df2.show()

    # df3 = df1.withColumn("year", expr("""
    #      case when year < 21 then cast(year as int) + 2000
    #      when year < 100 then cast(year as int) + 1900
    #      else year
    #      end"""))
    # df3.show()

    # df5 = df1.withColumn("day", col("day").cast(IntegerType())) \
    #      .withColumn("month", col("month").cast(IntegerType())) \
    #      .withColumn("year", col("year").cast(IntegerType())) 

    # df6 = df5.withColumn("year", expr("""
    #       case when year < 21 then year + 2000
    #       when year < 100 then year + 1900
    #       else year
    #       end"""))
    # df6.show()

    final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType())) \
        .withColumn("year", when(col("year") < 20, col("year") + 2000)
                    .when(col("year") < 100, col("year") + 1900)
                    .otherwise(col("year"))) \
        .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')")) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name", "dob"]) \
        .sort(col("dob").desc())

    final_df.show()

    