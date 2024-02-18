import os
import sys
import pyspark
import datetime
from pyspark.sql import SparkSession

print("Creating Spark Session")

HUDI_VERSION = '0.14.0'
SPARK_VERSION = '3.4'

SUBMIT_ARGS = f"--packages org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:{HUDI_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

print("check 1")

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

print("Spark Version")
print(spark.version)

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://abc.serveo.net:9000/")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

print("Creating mock data")

customer_data = [
    {
        "customer_id": '7af9fa7b-d749-4669-8340-71e9008abc8b',
        "name": 'Marco Phillips',
        "state": 'Ohio',
        "city": 'Port Mary',
        "email": 'jenniferwu@example.com',
        "created_at": '2024-02-17T17:30:16.321933',
        "address": 'PSC 6379, Box 8600\nAPO AP 11570',
        "salary": 46096
    },
    {
        "customer_id": 'a57fca99-2524-45ca-956b-f242789ce14b',
        "name": 'Jasmine Bates',
        "state": 'Wyoming',
        "city": 'Jamesborough',
        "email": 'brittanystewart@example.net',
        "created_at": '2024-02-17T17:30:16.322182',
        "address": '722 Jennings Station Suite 599\nTimothystad, MP 74938',
        "salary": 72263
    }
]

print("Mock data created")
print(customer_data)

spark_df_customers = spark.createDataFrame(data=[tuple(i.values()) for i in customer_data],
                                           schema=list(customer_data[0].keys()))
spark_df_customers.show(3)


def write_to_hudi(spark_df,
                  table_name,
                  db_name,
                  method='upsert',
                  table_type='COPY_ON_WRITE',
                  recordkey='',
                  precombine='',
                  partition_fields=''
                  ):
    path = f"s3a://huditest/hudi/database={db_name}/table_name={table_name}"

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': method,
        'hoodie.datasource.write.recordkey.field': recordkey,
        'hoodie.datasource.write.precombine.field': precombine,
        "hoodie.datasource.write.partitionpath.field": partition_fields,

        "hoodie.datasource.hive_sync.database": db_name,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,

    }
    print("hudi_options")
    print(hudi_options)
    print("\n")
    print("\n")
    print(path)
    print("\n")

    spark_df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save(path)


try:
    print("Try write ")
    write_to_hudi(
        spark_df=spark_df_customers,
        db_name="default",
        table_name="customers",
        recordkey="customer_id",
        precombine="created_at",
        partition_fields="state"
    )
    print("DONE")
except Exception as e:
    print("Error ** ")
    print(e)
