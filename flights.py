from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions",100)


data="C:/bigdata/drivers/flights.csv"



df1=spark.read.format("csv").option("header","true").option("inferSchema","true").option("header","true").load(data)



df1.show()
df1.printSchema()


df2=df1.withColumn("array",explode(array_distinct(split(col("dest"),"-"))))
df2.show()


df2.groupBy(col("name")).count().show()

df3=df2.groupBy("name").agg(count(col("name")))

df3.show()


print(df3.rdd.getNumPartitions())
