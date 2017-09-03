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

df.select('age')
#DataFrame[age: bigint]

type(df['age'])
#pyspark.sql.column.Column

df.select('age').show()
# +----+
# | age|
# +----+
# |null|
# |  30|
# |  19|
# +----+

type(df.select('age'))
# pyspark.sql.dataframe.DataFrame

df.head(2)
# [Row(age=None, name='Michael'), Row(age=30, name='Andy')]

df.head(2)[0]
# Row(age=None, name='Michael')

type(df.head(2)[0])
# pyspark.sql.types.Row

df.select(['age','name'])
# DataFrame[age: bigint, name: string]

df.select(['age','name']).show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+

df.withColumn('newAge',df['age'])
# DataFrame[age: bigint, name: string, newAge: bigint]

df.withColumn('newAge',df['age']).show()
# +----+-------+------+
# | age|   name|newAge|
# +----+-------+------+
# |null|Michael|  null|
# |  30|   Andy|    30|
# |  19| Justin|    19|
# +----+-------+------+

df.withColumnRenamed('age','my_new_age').show()
# +----------+-------+
# |my_new_age|   name|
# +----------+-------+
# |      null|Michael|
# |        30|   Andy|
# |        19| Justin|
# +----------+-------+

df.createOrReplaceTempView('people')
results = spark.sql('SELECT * FROM people')
results.show()
# +----+-------+
# | age|   name|
# +----+-------+
# |null|Michael|
# |  30|   Andy|
# |  19| Justin|
# +----+-------+
new_results = spark.sql('SELECT * FROM people WHERE age = 30')
new_results.show()
# +---+----+
# |age|name|
# +---+----+
# | 30|Andy|
# +---+----+

# contents of JSON FILE people.json
# {"name":"Michael"}
# {"name":"Andy", "age":30}
# {"name":"Justin", "age":19}