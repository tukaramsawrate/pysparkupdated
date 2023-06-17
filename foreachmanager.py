from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


"find each manager give me the employee having second heighest sal"
data2="C:\\bigdata\\drivers\\emp1.csv"


df2=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data2)

df3=df2.alias("foremp").join(df2.alias("formp"),col("foremp.empno")==col("formp.mgr"),"inner")\
    .select(col("foremp.empno").alias("manager"),col("foremp.sal").alias("mgrsal"),col("formp.empno").alias("employ"),col("formp.sal").alias("empsal"))

df3=df3.withColumn("dr",dense_rank().over(Window.partitionBy("manager").orderBy(col("empsal").desc()))).filter(col("dr")==2).drop("dr")
df2.show()
df3.show()