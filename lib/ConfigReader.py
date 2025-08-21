# This file reads the application configuration and sets up the PySpark environment.
import configparser
from pyspark import SparkConf

# This function reads the application configuration from a specified environment.
def get_app_config(env):
    config= configparser.ConfigParser()
    config.read('configs/application.conf')
    app_conf={}
    for(key, value) in config.items(env):
        app_conf[key] = value
    return app_conf

# This function sets up the PySpark configuration based on the environment.
def get_spark_conf(env):
    config=configparser.ConfigParser()
    config.read('configs/pyspark.conf')
    pyspark_conf= SparkConf()
    for(key,value) in config.items(env):
        pyspark_conf.set(key, value)
    return pyspark_conf
