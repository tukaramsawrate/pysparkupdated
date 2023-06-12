from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()



data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]


df1=spark.createDataFrame(data,columns)

df1.show()
df1.printSchema()

df1.groupby(col("Product")).sum("Amount").show()

df1.groupby(col("Product")).pivot("Country").sum("Amount").show()

df1.groupby("Country","Product").sum("Amount").show()


df1.groupby("Country").pivot("Product").sum("Amount").show()



#df1.write.mode("Overwrite").format("csv").option("header","true").partitionBy("C")