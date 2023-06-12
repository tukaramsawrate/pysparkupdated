from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data="C:/bigdata/drivers/booking_table.csv"
data1="C:/bigdata/drivers/usertable.csv"
df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df2=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data1)

df2.show()
df1.show()



df3=df1.withColumn("rank",rank().over(Window.partitionBy("user_id").orderBy(col("booking_date")))).where((col("rank")==1) & (col("line_of_business")=='Hotel'))

df3.show()

'''select segment, count(case when line_of_business='Hotel' then 1 end)hotel_booking,
count(case when line_of_business='Flight' then 1 end)flight_booking from user_table uk join booking_table uk1
on uk.user_id=uk1.user_id
group by segment;'''


df4=df1.join(df2,df1.user_id==df2.user_id,"inner")
df4=df4.groupBy("segment").agg(count(when(col("line_of_business")=='Flight',1)).alias("flight"),count(when(col("line_of_business")=='Hotel',1)).alias("Hotel"))
df4.show()


