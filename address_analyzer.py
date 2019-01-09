
import os
import pandas as pd
from pyspark.sql import SparkSession
import datetime, math
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import udf

# data = pd.read_csv(r"C:\Users\justinhu\addr.csv")
input_file = 'C:/Users/justinhu/ESTATEPHASE_NAME.json'
#input_file = 'C:/Users/justinhu/addr.csv'

master = 'local'
#master = 'spark://192.168.31.:9090'
spark = SparkSession.builder.appName('test').master(master).getOrCreate()
sc = spark.sparkContext

#rawdata = spark.read.text(input_file)
rawdata = sc.textFile(input_file)

#
'''
df = spark.read.format("csv").\
    option("header", "false").\
    option("delimiter", "\t"). \
    schema(AddrSchema).\
    option("inferSchema", "false").\
    load("friends.txt")


df = df.withColumn("fId11", func.when(df.fId1 > df.fId2, df.fId2).otherwise(df.fId1))\
    .withColumn("fId22", func.when(df.fId1 > df.fId2, df.fId1).otherwise(df.fId2))\
    .selectExpr("fId11 as fId1","fId22 as fId2")
zh
df1 = df.alias('a').join(df.alias('b'), col('a.fId1') == col('b.fId1')).\
    where(col('a.fId2') < col('b.fId2'))\
    .selectExpr('a.fId1 as common','a.fId2 as fId1','b.fId2 as fId2')

df1 = df1.groupby("fId1", "fId2").agg(func.collect_set("common").alias('common_set'))
df1.show()

#df1 which not in df
df2 = df1.join(df, ["fId1", "fId2"], "leftanti")
df2 = df2.withColumn('numFriends',array("fId2", size(col("common_set")))).\
    groupBy("fId1").\
    agg(func.collect_list("numFriends").alias('suggestion_set'))\


df2.show(20, False)
#df2.select("suggestion_set").show()
'''

wordSchema = StructType(
   [
    StructField("word", StringType(), True),
    StructField("count", IntegerType(), True)
   ]
)

#rawdata.collect()
#rawdata.count()
wordsRDD = rawdata.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)  ##.sortByKey()
dfw = spark.createDataFrame(wordsRDD, wordSchema)
#dfw = wordsRDD.toDF() 
dfw.createOrReplaceTempView("W")
dfr = spark.sql("select * from W order by count desc") 
pd_dfr = dfr.toPandas()

#pd_dfr = dfr.toPandas()
#spark_df = sc.createDataFrame(df)


#df = wordsRDD.collect()
#df = pd.DataFrame(df,columns=['word','count'])
#%%

output_file = 'C:/Py/addr_list.csv'
#dfr.rdd.map(lambda x:x(0)+"\t"+x(1)).saveAsTextFile(output_file)
pd_dfr.to_csv(output_file)


#%%
input_file = 'C:/Users/justinhu/addr.csv'
data = pd.read_csv(r"C:\Users\justinhu\addr.csv", header = 0)
tol_num = data.shape[0]
for i in range(tol_num):
    raw_addr = data.iloc[i]


#%%


# def csv_reader(file):
#     data = pd.read_csv("fileName.csv")
#
#
# raw_file_path = r'C:\Justin\APIS\buyerseller_excel_files\completed'
# list = os.listdir(raw_file_path)
# for i in range(0, len(list)):
#     csv_path = os.path.join(raw_file_path, list[i])
#     if os.path.isfile(raw_file_path) and os.path.splitext(raw_file_path)[1] == '.csv':
#         pass
