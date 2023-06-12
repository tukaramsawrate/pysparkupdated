from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data='C:\\bigdata\\drivers\\us-500.csv'


df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("mode","dropmalformed").load(data)\
    .withColumnRenamed("zip","sal").select("first_name",'state','sal')

win=Window.partitionBy(col("state")).orderBy(col("sal").desc())


res=df.withColumn("rn",rank().over(win))\
    .withColumn("dens",dense_rank().over(win))\
    .withColumn("rowno",row_number().over(win))\
    .where(col("dens")==2)

res.show()
res1=df.withColumn("percentrank",percent_rank().over(win))\
    .withColumn("grade",ntile(4).over(win))\
    .withColumn("lead",lead(col("sal"),1).over(win))\
    .withColumn("lag",lag(col("sal"),1).over(win)).na.fill(0)\
    .withColumn("diffsal",(col("sal")-col("lead")))\
    .withColumn("first",first(col("sal")).over(win))\
    .withColumn("diff",(col("sal")-col("first")))


res1.show()


#df.groupby('sal').agg(max('sal')).show()
