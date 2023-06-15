from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()






data1="C:\\bigdata\\drivers\\usersdata.csv"
data2="C:\\bigdata\\drivers\\ordersdata.csv"
data3="C:\\bigdata\\drivers\\itemsdata1.csv"
df1=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data1)
df2=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data2)
df3=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data3)
df1.show()
df2.show()
df3.show()

df4=df2.withColumn("dr",dense_rank().over(Window.partitionBy("seller_id").orderBy(col("order_data").asc())))
df5=df4.join(df3,"item_id","inner")
df5=df5.join(df1,df5.seller_id==df1.user_id,"inner")
df5=df5.withColumn("yesorno",when(col("favorite_brand")==col("item_brand"),"yes").otherwise("no")).filter(col("dr")==2).drop("dr","order_id")
df5.show()