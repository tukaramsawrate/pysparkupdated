from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data2="C:\\bigdata\\drivers\\salemp.csv"


df1=spark.read.format("csv").option("header","true").option("inferSchema","true").option("mode","dropmalformed").load(data2)


df1.show()

df1=df1.groupBy("empid").agg(sum(when(col("salary_cmp_type")=='salary',col('val'))).alias("sal"),sum(when(col("salary_cmp_type")=='bonous',col("val")))
                             .alias("bonous"),sum(when(col("salary_cmp_type")=='hike_perct',col("val"))).alias("hike_perct"))

df1.show()
