import pyspark
from pyspark.sql import SparkSession
file1 = open('maven-downloads.txt', 'r')
packages = file1.readlines()
spark = SparkSession.builder\
   .master("local")\
   .appName("dependecies")\
   .config("spark.jars.packages", ",".join(packages))\
   .config("spark.jars.ivy", "/opt/airflow/jars")\
   .getOrCreate()