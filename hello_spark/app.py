from pyspark.sql import *
from lib.logger import Log4j

if __name__ == "__main__":
   spark = SparkSession.builder \
        .master("local[3]") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark") \
        .appName("Hello Spark") \
        .getOrCreate()
   
   logger = Log4j(spark)

   logger.info("Starting HelloSpark")

   logger.info("Finished HelloSpark")
   
   spark.stop()