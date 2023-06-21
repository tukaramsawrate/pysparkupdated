from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()



data2="C:\\bigdata\\flipkartdelivary.csv"


df1=spark.read.format("csv").option("inferSchema","true").option("dropmalformed","mode").option("header","true").load(data2)
#col=[re.sub("[^a-zA-Z0-9]","",i)for i in df1.columns]
#df1=df1.toDF(*col)

df1=df1.groupby("city").agg(array_distinct(collect_list(col("item"))).alias("items"))
df1.show()
