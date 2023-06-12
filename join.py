from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]

empcolumns=["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]



df1=spark.createDataFrame(data=emp,schema=empcolumns)

df1.show()
df1.printSchema()
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]



deptColumns = ["dept_name","dept_id"]


df2=spark.createDataFrame(data=dept,schema=deptColumns)

df2.show()
df2.printSchema()


res=df1.join(df2,df1.emp_dept_id==df2.dept_id,"inner")
res.show()



df1.join(df2,df1.emp_dept_id==df2.dept_id,"left").show()


df1.join(df2,df1.emp_dept_id==df2.dept_id,"right").show()


df1.join(df2,df1.emp_dept_id==df2.dept_id,'fullouter').show()

df1.join(df2,df1.emp_dept_id==df2.dept_id,"leftanti").show()

df1.join(df2,df1.emp_dept_id==df2.dept_id,"leftsemi").show()




