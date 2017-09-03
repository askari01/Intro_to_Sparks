import findspark
# change path to your spark folder
findspark.init('/home/syedaskari/spark-2.2.0-bin-hadoop2.7')

from pyspark.sql import SparkSession

# name of app Basics
spark = SparkSession.builder.appName('Basics').getOrCreate()

# path to json file
df = spark.read.json('/home/syedaskari/Downloads/Udemy - Spark and Python for Big Data with PySpark/01 Introduction to Course/attached_files/002 Course Overview/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/people.json')

df.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

# prints schema
df.printSchema()
# root
# |-- age: long (nullable = true)
# |-- name: string (nullable = true)

df.columns
#  ['age', 'name']  

df.describe()
# DataFrame[summary: string, age: string, name: string]

df.describe().show()
# +-------+------------------+-------+
# |summary|               age|   name|
# +-------+------------------+-------+
# |  count|                 2|      3|
# |   mean|              24.5|   null|
# | stddev|7.7781745930520225|   null|
# |    min|                19|   Andy|
# |    max|                30|Michael|
# +-------+------------------+-------+


from pyspark.sql.types import (StructField,IntegerType,
                               StringType,StructType)
data_schema = [StructField('age', IntegerType(), True),
              StructField('name', StringType(), True)]

final_struc = StructType(fields=data_schema)

dy = spark.read.json('/home/syedaskari/Downloads/Udemy - Spark and Python for Big Data with PySpark/01 Introduction to Course/attached_files/002 Course Overview/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/people.json',schema=final_struc)

dy.printSchema()
# root
# |-- age: integer (nullable = true)
# |-- name: string (nullable = true)

# contents of JSON FILE people.json
# {"name":"Michael"}
# {"name":"Andy", "age":30}
# {"name":"Justin", "age":19}