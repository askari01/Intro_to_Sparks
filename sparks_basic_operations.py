import findspark
# change path to your spark folder
findspark.init('/home/syedaskari/spark-2.2.0-bin-hadoop2.7')

from pyspark.sql import SparkSession

# name of app Basics
spark = SparkSession.builder.appName('Basics').getOrCreate()

# path to CSV file
df = spark.read.csv('/home/syedaskari/Downloads/Udemy - Spark and Python for Big Data with PySpark/01 Introduction to Course/attached_files/002 Course Overview/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/appl_stock.csv',inferSchema=True,header=True)

df.show()

df.head(3)[0]
# Row(Date=datetime.datetime(2010, 1, 4, 0, 0), Open=213.429998, High=214.499996, Low=212.38000099999996, Close=214.009998, Volume=123432400, Adj Close=27.727039)

df.printSchema()

df.filter("close < 500").show()

df.filter("close < 500").select('open').show()

df.filter("close < 500").select(['open','close']).show()

df.select( (df['open'] > 200 ) & ( df['close'] <200 ) ).show()

df.filter(df['Low']==197.16).show()

result = df.filter(df['Low']==197.16).collect()

row = result[0]

row.asDict()

row.asDict()["Volume"]