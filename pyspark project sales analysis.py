# Databricks notebook source
 /FileStore/tables/sales_csv.txt

  /FileStore/tables/menu_csv.txt

 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql.functions import *

schema=StructType([

    StructField("product_id",IntegerType(),True),
    StructField("custmer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True),

])

sales_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv.txt")
sales_df.display()

# COMMAND ----------

sale=spark.read.option("inferSchema","true").schema(schema).csv("/FileStore/tables/sales_csv.txt",header=True)

# COMMAND ----------

sale.show(10)

# COMMAND ----------

from pyspark.sql.functions import month,year,quarter

# COMMAND ----------

sales_df=sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df=sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
sales_df.show()


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema=StructType([

    StructField("product_id",IntegerType(),True),
    StructField("order_name",StringType(),True),
    StructField("product_prise",StringType(),True),
    

])

menu_df=spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/menu_csv.txt")
menu_df.show()

# COMMAND ----------

# DBTITLE 1,Total amount spent by each customer
df=sales_df.join(menu_df,"product_id")
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df=df.withColumn("product_prise",df["product_prise"].cast("int"))
df1.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1=df.groupBy("custmer_id").agg(sum("product_prise").alias("Total_amount_spent"))
df1.show()


# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,Total amount spend by each food categiry
total_amount_spent1=df.groupBy("order_name").agg(sum("product_prise")).orderBy("order_name")
total_amount_spent1.show()

# COMMAND ----------

df.show()

# COMMAND ----------

total_amount_spent.printSchema()

# COMMAND ----------

df = df.withColumn("product_prise",df["product_prise"].cast("Int")) 

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Total Amount sales in each month
total_amount_spent1=df.groupBy("order_month").agg(sum("product_prise").alias("   prise   "))
total_amount_spent1.show()

# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,yearly sales

df=df.withColumn("year",year(df.order_date))
df.show()

# COMMAND ----------

yearly_sales_df5=df.groupBy("year").sum("product_prise").orderBy("year")
yearly_sales_df5.show()



# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,Quaterly sales
sales_df6=df.groupBy("order_quarter").sum("product_prise").orderBy("order_quarter")
sales_df6.show() 

# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,how many time each product perchase
sales_df7=df.groupBy("order_name").agg(count("order_date").alias("Orders")).orderBy("Orders",ascending=0)
sales_df7.show()

# COMMAND ----------

df.show() 

# COMMAND ----------

# DBTITLE 1,top 5 oreder item
sales_df7=df.groupBy("order_name").agg(count("order_date").alias("Orders")).orderBy("Orders",ascending=0).limit(5)
sales_df7.show() 

# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,Frequncy of customer visited on Restaurant
from pyspark.sql.functions import *
sales_df8=sales_df.filter(sales_df.source_order=="Restaurant").groupBy("product_id").agg(countDistinct("order_date"))
sales_df8.show()
                                                                              


# COMMAND ----------

sales_df1.show()

# COMMAND ----------

# DBTITLE 1,Total sales by each country
sales_df9=df.groupby("location").agg(sum("product_prise"))
sales_df9.show()

# COMMAND ----------

sales_df1.show()

# COMMAND ----------

# DBTITLE 1,total sales by order source
sales_df10=df.groupBy("source_order").agg(sum("product_prise"))
sales_df10.show()
