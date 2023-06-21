from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data2="C:\\bigdata\\drivers\\uber.csv"
data3="C:\\bigdata\\drivers\\uber_driversinfo.csv"

df1=spark.read.format("csv").option("inferSchema","true").option("header","true").option("mode","dropmalformed").load(data2)
col1=[re.sub('[^a-zA-Z]',"",i) for i in df1.columns]
df1=df1.toDF(*col1)
df1=df1.withColumn("Date",when(col("Date").isNull(),0).otherwise(col("Date")))
df2=spark.read.format("csv").option("inferSchema","true").option("header","true").option("mode","dropmalformed").load(data3)

col2=[re.sub("[^A-Za-z]","",i)for i in df2.columns]

df2=df2.toDF(*col2)
df1.show()
df2.show()

df6=df1.groupBy("Date","TimeLocal").agg(max(col("Requests")).alias("maxiumrequest"))
df6.show()
df3=df1.join(df2,df1.CompletedTrips==df2.TripsCompleted,"inner")

df4=df3.groupby("UniqueDrivers").agg(mean(col("Rating")))

df3.show()
df4.show()


df7=df2.groupBy("Name").agg(sum(col("TripsCompleted")))
df8=df2.groupBy("Name").agg(mean(col("Rating")))
df7.show()
df8.show()


df10=df2.withColumn("avg_ranting",avg("Rating").over(Window.partitionBy("Name"))).\
    withColumn("rank",rank().over(Window.orderBy(col("avg_ranting").desc()))).filter(col("rank")<=5)

df10.show()