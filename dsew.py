from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data1="C:\\bigdata\\drivers\\vce.csv"



df1=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data1)

df2=df1.groupby("bpid").agg(collect_list(col("custid")))
df3=df1.join(df2,"bpid","inner").withColumnRenamed("custid","newidno")
df1.show()
df2.show()
df3.show()