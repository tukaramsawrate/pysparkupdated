from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data1="C:\\bigdata\\drivers\\playerdata.csv"

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data1)

df1=df1.groupby("player_id").agg(min(col("event_date")).alias("first_login")).orderBy(col("player_id"))
df1.show()

data2="C:\\bigdata\\drivers\\tempereturedata.csv"

df2=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data2)



df2.show()



df3=df2.alias("ff").join(df2.alias("sf"),col("ff.temperature").alias("temp1")>col("sf.temperature").alias("temp2"),"inner")\
    .withColumn("diff",datediff(to_date(col("ff.recorddate"),"dd-MM-yyyy"),to_date(col("sf.recorddate"),"dd-MM-yyyy"))).where(col("diff")==1)

df3.show()





