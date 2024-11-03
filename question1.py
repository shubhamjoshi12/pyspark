from datetime import datetime,timedelta
import os
import sys

from pyspark.sql.functions import when, to_date, date_sub, current_date, col, initcap

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("shubham") \
    .master("local[*]") \
    .getOrCreate()

employees = [
("karthik", "2024-11-01"),
("neha", "2024-10-20"),
("priya", "2024-10-28"),
("mohan", "2024-11-02"),
("ajay", "2024-09-15"),
("vijay", "2024-10-30"),
("veer", "2024-10-25"),
("aatish", "2024-10-10"),
("animesh", "2024-10-15"),
("nishad", "2024-11-01"),
("varun", "2024-10-05"),
("aadil", "2024-09-30")
]
employees_df = spark.createDataFrame(employees, ["name", "last_checkin"])
employees_df=employees_df.withColumn('last_checkin',to_date("last_checkin","yyyy-MM-dd"))\
                         .select(initcap("name"),'last_checkin',when('last_checkin'>=date_sub(current_date(),7),"Active")
                         .otherwise("Inactive").alias('Status')).show()

employees_df.createOrReplaceTempView('details')
spark.sql("""select INITCAP(name), last_checkin, case
WHEN last_checkin>=now()-interval 7 days THEN 'Active'
else 'Inactive'
end AS Status
from details""").show()




