from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

setting = spark.conf.get("spark.master")
if "local" in setting:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
else:
    print("Do nothing - dbutils should be available already")


print("\n".join(map(str, dbutils.fs.ls("/"))))
