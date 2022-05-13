# Databricks notebook source
from pyspark.sql import SparkSession
sc = SparkSession.builder.appName("RDDLearning").getOrCreate()
rddlist = ['a','b','c']
rdd1 = sc.sparkContext.parallelize(rddlist)
print (rdd1.collect())


# COMMAND ----------

from pyspark.sql import SparkSession
from operator import add
sc = SparkSession.builder.appName("lambda").getOrCreate()
llist = [ 10, 20, 30 ]
rddn = sc.sparkContext.parallelize(llist)
frdd = rddn.map(lambda x : x + 10)
crdd = rddn.reduce(add)

print ("lambda", frdd.collect())
print ("reduce", crdd)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
sc = SparkSession.builder.appName("Data Frame").getOrCreate()
headers1 = [ 'Sl No', 'Name', 'Marks']
data1 = [ (1, 'Ram', 90), (2, 'Lakshman', 95), (3, 'Narasimhan', 100)]
rdd1 = sc.sparkContext.parallelize(data1)
df1 = rdd.toDF(headers1)
headers2 = [ 'Name' , 'Country']
data2 = [ ('Ram', 'India'),('Narasimhan', 'World')]
rdd2 = sc.sparkContext.parallelize(data2)
df2 = rdd2.toDF(headers2)
df3 = df1.join(df2, df1.Name == df2.Name, "right").sort(col('Sl No')).show()

