from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data1="C:\\bigdata\\drivers\\teamdata.csv"


df1=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data1)


df1.show()

df2=df1.withColumn("wincnt",when(col("team_1")==col("winner"),1).otherwise(0)).withColumnRenamed("team_1","team").select("team","wincnt")
#df2=df2.select("team","wincnt")

df3=df1.withColumn("wincnt",when(col("team_2")==col("winner"),1).otherwise(0)).withColumnRenamed("team_2","team").select("team","wincnt")
df4=df2.unionAll(df3)
df4=df4.groupBy("team").agg(count(col("team")).alias("totalpalyed"),sum(col("wincnt")).alias("wincnt"),(count(col("team"))-sum(col("wincnt"))).alias("loss"))
df4.show()