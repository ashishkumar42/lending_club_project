from pyspark.sql import SparkSession
from lib.ConfigReader import get_spark_conf

def get_spark_session(env):
    conf = get_spark_conf(env)
    builder = SparkSession.builder.config(conf=conf) \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.permissions.enabled", "false") \
        .config("spark.hadoop.mapreduce.job.run-local", "true")
    return builder.getOrCreate()