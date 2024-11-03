from datetime import datetime,timedelta
import os
import sys

from pyspark.sql.functions import when, to_date, date_sub, current_date, col, initcap,sum

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("sales") \
    .master("local[*]") \
    .getOrCreate()

sales = [
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)
]
sales_df = spark.createDataFrame(sales, ["name", "total_sales"])
new_sales_df=sales_df.select(initcap("name"),"total_sales",when(col('total_sales')>50000,"Excellent")\
                .when((col('total_sales')>=25000) & (col('total_sales')<=50000),'Good')\
                .when(col('total_sales')<25000,"Needs Improvement").alias("performance_status")\
                )

totalsalesdf=new_sales_df.groupBy(col('performance_status')).agg(sum(col('total_sales')).alias('Total'))

totalsalesdf.show()