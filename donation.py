from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data="C:\\bigdata\drivers\\donations.csv"



df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("mode","dropmalformed").load(data)


#df.show()


df1=df.withColumn("todayd",current_date()).withColumn("cts",current_timestamp()).withColumn("dt",to_date(col("dt"),'d-M-yyyy'))\
    .withColumn("dateadd",date_add(current_date(),5)).withColumn("datesub",date_sub(current_date(),5))\
    .withColumn("datediff",datediff(current_date(),col("dt"))).withColumn("addmonths",add_months(current_date(),2))\
    .withColumn("nextday",next_day(current_date(),'Sat')).withColumn("lastday",last_day(current_date()))\
    .withColumn("coldiff",col("todayd")-col("dt")).withColumn("dayofyear",dayofyear(current_date()))\
    .withColumn("dayofmonth",dayofmonth(current_date())).withColumn("dayofweek",dayofweek(current_date()))\
    .withColumn("yearofweek",weekofyear(current_date())).withColumn("dateformat",date_format(current_date(),"d-M-yy Q dd E EEEE MMMM MM MMM H:m:s.SSSS"))\
    .withColumn("monthtrunc",date_trunc("month",col("dt"))).withColumn("yeartruc",date_trunc("year",col("dt")))\
    .withColumn("daytruc",date_trunc("day",col("dt"))).withColumn("hourtrunc",date_trunc("hour",current_timestamp()))\
    .withColumn("minutetrunc",date_trunc("minute",current_timestamp())).withColumn("lastdayinabr",date_format(last_day(col("dt")),'E'))\
    .withColumn("lastdaydate",next_day(date_sub(last_day(current_date()),7),'Mon'))\
    .withColumn("quarter",date_trunc("quarter",current_timestamp())).withColumn("week",date_trunc("week",col("dt"))).\
    withColumn("truncmm",trunc(col("dt"),'MM')).withColumn("truncyy",trunc(col("dt"),'yy')).\
    withColumn("monthsbetween",months_between(current_date(),col("dt"))).withColumn("totimestamp",to_timestamp(col("todayd"),"d-M-yyyy HH:mm:ss:SSSS")).\
    withColumn("hour",hour(col("cts"))).withColumn("year",year(col("cts"))).withColumn("month",month(col("dt")))\
    .withColumn("second",second(col("cts"))).withColumn("dayofweek",dayofweek(col("dt"))).\
    withColumn("quarter11",quarter(col("dt")))


df1.show()


