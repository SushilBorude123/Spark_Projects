# Databricks notebook source
salesdf=spark.read.option("inferSchema","true").option("sep",",").csv("dbfs:/FileStore/employee.csv",header=True)

# COMMAND ----------

salesdf.show()


# COMMAND ----------

salesdf.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType,IntegerType


EmpSchema = StructType([   StructField('Empno', IntegerType(), True), 
    StructField('Empname', StringType(), True),
    StructField('Salary', IntegerType(), True) 
])

# COMMAND ----------


empdf=spark.read.schema(EmpSchema).csv("dbfs:/FileStore/employee.csv",header=True)

# COMMAND ----------

empdf.printSchema()

# COMMAND ----------

empdf.show()

# COMMAND ----------


empdf=spark.read.schema(EmpSchema).option("mode","DropMalformed").csv("dbfs:/FileStore/employee.csv",header=True)

# COMMAND ----------

empdf.show()

# COMMAND ----------

empdf.write.mode("overwrite").saveAsTable('employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update employees set Salary=8787 where Empno=121;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employees where Empname="aaa";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

salesdf=spark.read.option("inferSchema","true").option("sep",",").csv("dbfs:/FileStore/superstore.csv",header="True")
salesdf

# COMMAND ----------

salesdf.show()

# COMMAND ----------

salesdf=spark.read.option("inferSchema","true").option("sep",",").csv("dbfs:/FileStore/superstore.csv",header=True)


# COMMAND ----------

salesdf.printSchema()

# COMMAND ----------

salesdf.dtypes


# COMMAND ----------

salesdf.select("country","state").where("country='India'").show()



# COMMAND ----------

salesdf.select("country","state").where("Country='India'").distinct().show()

# COMMAND ----------

salesdf.select("country","state").where("Country='India'").distinct()



# COMMAND ----------

salesdf_Ind_df=salesdf.select("country","state").where("country='India'").distinct()
salesdf_Ind_df.show()

# COMMAND ----------

salesdf_Ind_df=salesdf.select("country","state").where("country='India'").dropDuplicates(["country"])
salesdf_Ind_df.show()



# COMMAND ----------

salesdf_Ind_df=salesdf.select("country","state").where("country='India'").dropDuplicates(["country"]).show()
salesdf_Ind_df


# COMMAND ----------

salesdf.select("country","state","profit").show()

# COMMAND ----------

from pyspark.sql.functions import col
salesdf.select(col("profit").cast("int"))

# COMMAND ----------

from pyspark.sql.functions import *
salesdf.select(col("profit").cast("int"))

# COMMAND ----------

 salesdf= salesdf.withColumn("profit",col("profit").cast("int"))

# COMMAND ----------

salesdf.printSchema()

# COMMAND ----------

salesdf.groupBy("country","state").count().show()


# COMMAND ----------

salesdf.filter("country='India'").groupBy("country","state").count()

# COMMAND ----------

salesdf.filter("country='United States'").groupBy("country","state").sum("profit").show()}

# COMMAND ----------

salesdf.groupBy("country").agg(avg("profit").alias ("A"),sum("profit").alias("S")).show()


# COMMAND ----------


salesdf.select("country","state").where("Country='India'").orderBy("state",asending=True).show()

# COMMAND ----------

salesdf.select("country","state").distinct().where("Country='India'").orderBy("state",asending=True).show()

# COMMAND ----------

salesdf.select("country","state").distinct().where("Country='United States'").sort(col("state").desc()).show()

# COMMAND ----------

salesdf.createOrReplaceGlobalTempView("sales")

# COMMAND ----------

salesdf.createOrReplaceTempView("sales")

# COMMAND ----------

spark.sql("show tables").show()

# COMMAND ----------

spark.sql("select country,state,sum(profit) as totalprofit from sales where country='United States' group by country,state").show()

# COMMAND ----------

salesdf.write.csv("/FileStore/target/salescsv")

# COMMAND ----------

salesdf.rdd.getNumPartitions()

# COMMAND ----------

salesdf.write.parquet("/fileStore/target/salesparq")

# COMMAND ----------

salesdf.write.saveAsTable("sales_perm")

# COMMAND ----------

spark.sql("show tables").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select country,state,sum(profit) as totalprofit from sales where country='United States' group by country,state

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("LogStreaming").getOrCreate()
lgfp="dbfs:/FileStore/networkevent.log"


# COMMAND ----------

log_sc="ip STRING,ss string,rr string,timestamp STRING,url STRING,statuscode Integer,Bytes Integer"

# COMMAND ----------

lsdf=spark.read.text(lgfp)

# COMMAND ----------

sel_col=lsdf.select([col(ip) for ip in lsdf.columns if "[%d%d.%d%d%d.%d%d.%d%d]" in ip])

# COMMAND ----------

sel_col.show()

# COMMAND ----------


