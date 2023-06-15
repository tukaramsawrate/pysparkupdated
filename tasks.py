from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data3="C:\\bigdata\\drivers\\tasks.csv"

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data3)



df1=df1.withColumn("date_value",to_date(col("date_value"),"dd-mm-yyyy")).\
    withColumn("rn",to_date(col("date_value"),"dd-mm-yyyy")-1*row_number().over(Window.partitionBy("state").orderBy("date_value")))

df1=df1.groupby("state","rn").agg(min(col("date_value")).alias("startdate"),max(col("date_value")).alias("enddate")).drop("rn")
df1.show()