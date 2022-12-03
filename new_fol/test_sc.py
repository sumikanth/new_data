import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
'''
Stuff i have learned - 
we need to create dockerfile.spark and add in requirements dependencies to it
'''

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")
rdd = spark.sparkContext.parallelize(data)
df = spark.createDataFrame(rdd).toDF(*columns)
df.show()

def audit_check(target_column,original_Cols):
    if target_column in original_Cols:
        return target_column,target_column+"_new"
    return target_column,target_column


def audit_udf(column_transform, prev_value,new_value):
    '''
    Each argument triple is of the format col_name:{column_Transform}, prev_value, new_value
    '''
    ret_string=str({"Column_Transform":column_transform,"Previous":None,"New":None})
    if prev_value!=new_value:
        ret_string=str({"Column_Transform":column_transform,"Previous":prev_value,"New":new_value})
    return ret_string

def audit_udf_call(column_transform: pd.Series, prev_value: pd.Series,new_value: pd.Series) -> pd.Series:
    return pd.Series(map(audit_udf,column_transform, prev_value,new_value))

audit = pandas_udf(audit_udf_call, returnType=StringType())

#"len(df['language'])<=4","lit('short')"
def conditional_change(df, target_column, condition, choice):
    org_val=col(target_column) if target_column in df.columns else lit(None)
    old_target,target_column=audit_check(target_column, df.columns)
    df=df.withColumn(target_column,when(eval(condition),eval(choice)).otherwise(org_val))
    #add guid to that column name
    df=df.withColumn("audit_"+"conditional_change_"+old_target,audit(lit("conditional_change_"+old_target),col(old_target),col(target_column)))
    df=df.drop(old_target)
    df=df.withColumnRenamed(target_column,old_target)
    return df

print("here")
df= conditional_change(df,'language',"df['language']=='Java'","lit('hi')")
df= conditional_change(df,'users_count',"df['users_count']=='20000'","lit('ohno')")
union_df=None
for x in df.columns:
    if x.startswith("audit_"):
        if union_df is None:
            union_df=df.select(x)
        else:
            union_df=union_df.union(df.select(x))

union_df.show(truncate=False)
schema = StructType(
    [
        StructField('Column_Transform', StringType(), True),
        StructField('Previous', StringType(), True),
        StructField('New', StringType(), True)
    ]
)
union_df=union_df.withColumn("val",from_json(union_df.columns[0],schema)).select(col("val.*")).na.drop("all")
union_df.show()
df=df.drop(*[x for x in df.columns if x.startswith("audit_")])
df.show()
# @pandas_udf
# def audit_cols()
# ### TODO: class to read in the spark stream from kafka and extract to a dataframe
# ### Transform and log the changes
















df.write.mode("overwrite").format("csv").save("hdfs://namenode:9000/hadoop/data/data/test.csv")

