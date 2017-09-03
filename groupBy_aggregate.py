import findspark
# change path to your spark folder
findspark.init('/home/syedaskari/spark-2.2.0-bin-hadoop2.7')

from pyspark.sql import SparkSession

# name of app Basics
spark = SparkSession.builder.appName('Basics').getOrCreate()

# path to CSV file
df = spark.read.csv('/home/syedaskari/Downloads/Udemy - Spark and Python for Big Data with PySpark/01 Introduction to Course/attached_files/002 Course Overview/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/sales_info.csv',inferSchema=True,header=True)

df.show()

df.printSchema()

df.groupBy('Company')

df.groupBy('Company').mean().show()

df.groupBy('Company').count().show()

df.groupBy('Sales').max().show()

df.groupBy('Company').max().show()

df.groupBy('Company').min().show()

df.groupBy('Company').sum().show()

df.agg({'Sales':'sum'}).show()

df.agg({'Sales':'max'}).show()

group_data = df.groupBy('Company')

group_data.agg({'Sales':'max'})

group_data.agg({'Sales':'max'}).show()

from pyspark.sql.functions import countDistinct,avg,stddev

df.select(countDistinct('Sales')).show()

df.select(avg('Sales')).show()

df.select(avg('Sales').alias('Average')).show()

df.select(stddev('sales').alias("Standard Dev.")).show()

from pyspark.sql.functions import format_number

sales_std = df.select(stddev('sales').alias("Standard Dev"))

sales_std.show()

sales_std.select(format_number('Standard Dev', 2).alias("std")).show()

df.show()

df.orderBy('Sales').show()

df.orderBy(df['Sales'].desc()).show()