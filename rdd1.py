from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()



sc=spark.sparkContext

data="C:\\bigdata\drivers\\email.txt"

rdd1=sc.textFile(data)
first=rdd1.first()
df1=rdd1.filter(lambda x:"@" in x).map(lambda x:x.strip(" ")).map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1])).toDF(["name","email"])
df1.show()

data2="C:\\bigdata\drivers\\asl.csv"
rdd2=sc.textFile(data2)
fir=rdd2.first()
rdd3=rdd2.filter(lambda x:x!=fir).map(lambda x:x.split(',')[-1]).filter(lambda x:x=='blr')
for i in rdd3.collect():
    print(i)

rdd4=rdd2.filter(lambda x:x!=fir).map(lambda x:x.split(',')).filter(lambda x:int(x[1])>=30)
for i in rdd4.collect():
    print(i)

data3="C:\\bigdata\drivers\\donations.csv"
rdd3=sc.textFile(data3)



fir6=rdd3.first()
rdd4=rdd3.filter(lambda x:x!=fir6).map(lambda x:x.split(',')).map(lambda x:(x[0],int(x[-1]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[-1])
for i in rdd4.collect():
    print(i)


