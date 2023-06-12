from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data1="C:\\bigdata\\drivers\\entriesdata.csv"


df1=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data1)
df1.show()
#df1=df1.withColumn("dr",count("floor").over(Window.partitionBy("name","floor")))
df2=df1.groupby("name","floor").agg(count(col("floor")).alias("cnt"))

df3=df1.groupby("name").agg(count(col("floor")).alias("cnt"))
df2=df2.withColumn("rank",rank().over(Window.partitionBy("name").orderBy(col("cnt").desc()))).filter(col("rank")==1)
df2=df2.join(df3,df2.name==df3.name,"inner").drop("rank")
df2.show()
