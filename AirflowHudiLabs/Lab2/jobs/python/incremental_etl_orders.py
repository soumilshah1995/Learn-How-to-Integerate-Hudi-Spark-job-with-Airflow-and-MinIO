try:
    import ast, sys, datetime, re, os, json
    from ast import literal_eval
    from datetime import datetime
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext

    from dataclasses import dataclass
    import boto3

    print("All modules Loaded")
except Exception as e:
    print("Error in Imports ")

global minio_endpoint, \
    minio_access_key, \
    minio_secret_key, \
    BUCKET_NAME, \
    loaders_json_payload, \
    spark

minio_endpoint = 'http://minio:9000/'
minio_access_key = 'admin'
minio_secret_key = 'password'
minio_secure = False

loaders_json_payload = {
    "check_point_buckets": "huditest",
    "source": [
        {
            "source_type": "HUDI",  # HUDI | DYNAMODB
            "table_name": "orders",
            "spark_table_name": "orders",  # Creates TempView with this table name
            "path": "s3a://huditest/silver/table_name=orders/",
            "type": "INC"  # INC | FULL
        },
        {
            "source_type": "HUDI",  # HUDI | DYNAMODB
            "table_name": "customers",
            "spark_table_name": "customers",  # Creates TempView with this table name
            "path": "s3a://huditest/silver/table_name=customers/",
            "type": "FULL"  # INC | FULL
        },

    ]
}
BUCKET_NAME = loaders_json_payload.get("check_point_buckets")

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):

        self.BucketName = bucket
        self.client = boto3.client("s3",
                                   endpoint_url=minio_endpoint,
                                   aws_access_key_id=minio_access_key,
                                   aws_secret_access_key=minio_secret_key,
                                   verify=minio_secure

                                   )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


@dataclass
class HUDISettings:
    """Class for keeping track of an item in inventory."""

    table_name: str
    path: str


class HUDIIncrementalReader(AWSS3):
    def __init__(self, bucket, hudi_settings, spark_session):
        AWSS3.__init__(self, bucket=bucket)
        if type(hudi_settings).__name__ != "HUDISettings": raise Exception("please pass correct settings ")
        self.hudi_settings = hudi_settings
        self.spark = spark_session

    def __check_meta_data_file(self):
        """
        check if metadata for table exists
        :return: Bool
        """
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        return self.item_exists(Key=file_name)

    def __read_meta_data(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"

        return ast.literal_eval(self.get_item(Key=file_name).decode("utf-8"))

    def __push_meta_data(self, json_data):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.put_files(
            Key=file_name, Response=json.dumps(json_data)
        )

    def clean_check_point(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.delete_object(Key=file_name)

    def __get_begin_commit(self):
        self.spark.read.format("hudi").load(self.hudi_settings.path).createOrReplaceTempView("hudi_snapshot")
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_snapshot order by commitTime asc").limit(
            50).collect()))

        """begin from start """
        begin_time = int(commits[0]) - 1
        return begin_time

    def __read_inc_data(self, commit_time):
        incremental_read_options = {
            'hoodie.datasource.query.type': 'incremental',
            'hoodie.datasource.read.begin.instanttime': commit_time,
        }
        incremental_df = self.spark.read.format("hudi").options(**incremental_read_options).load(
            self.hudi_settings.path).createOrReplaceTempView("hudi_incremental")

        df = self.spark.sql("select * from  hudi_incremental")

        return df

    def __get_last_commit(self):
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_incremental order by commitTime asc").limit(
            50).collect()))
        last_commit = commits[len(commits) - 1]
        return last_commit

    def __run(self):
        """Check the metadata file"""
        flag = self.__check_meta_data_file()
        """if metadata files exists load the last commit and start inc loading from that commit """
        if flag:
            meta_data = json.loads(self.__read_meta_data())
            print(f"""
            ******************LOGS******************
            meta_data {meta_data}
            last_processed_commit : {meta_data.get("last_processed_commit")}
            ***************************************
            """)

            read_commit = str(meta_data.get("last_processed_commit"))
            df = self.__read_inc_data(commit_time=read_commit)

            """if there is no INC data then it return Empty DF """
            if not df.rdd.isEmpty():
                last_commit = self.__get_last_commit()
                self.__push_meta_data(json_data=json.dumps({
                    "last_processed_commit": last_commit,
                    "table_name": self.hudi_settings.table_name,
                    "path": self.hudi_settings.path,
                    "inserted_time": datetime.now().__str__(),

                }))
                return df
            else:
                return df

        else:

            """Metadata files does not exists meaning we need to create  metadata file on S3 and start reading from begining commit"""

            read_commit = self.__get_begin_commit()

            df = self.__read_inc_data(commit_time=read_commit)
            last_commit = self.__get_last_commit()

            self.__push_meta_data(json_data=json.dumps({
                "last_processed_commit": last_commit,
                "table_name": self.hudi_settings.table_name,
                "path": self.hudi_settings.path,
                "inserted_time": datetime.now().__str__(),

            }))

            return df

    def read(self):
        """
        reads INC data and return Spark Df
        :return:
        """

        return self.__run()


def load_hudi_tables(loaders):
    """load Hudi tables """

    for items in loaders.get("source"):
        table_name = items.get("table_name")
        path = items.get("hudi_path")

        if items.get("type") == "FULL":
            spark.read.format("hudi").load(path).createOrReplaceTempView(table_name)

        if items.get("type") == "INC":
            helper = HUDIIncrementalReader(
                bucket=BUCKET_NAME,
                hudi_settings=HUDISettings(
                    table_name=table_name,
                    path=path
                ),
                spark_session=spark
            )
            spark_df = helper.read()
            spark_df.createOrReplaceTempView(table_name)


def load_hudi_tables(loaders):
    """load Hudi tables """

    for items in loaders.get("source"):
        table_name = items.get("table_name")
        path = items.get("hudi_path")

        if items.get("type") == "FULL":
            spark.read.format("hudi").load(path).createOrReplaceTempView(table_name)

        if items.get("type") == "INC":
            helper = HUDIIncrementalReader(
                bucket=BUCKET_NAME,
                hudi_settings=HUDISettings(
                    table_name=table_name,
                    path=path
                ),
                spark_session=spark
            )
            spark_df = helper.read()
            spark_df.createOrReplaceTempView(table_name)


class HudiLoader(object):
    def __init__(self,
                 table_name,
                 hudi_path,
                 spark_session,
                 check_points_bucket,
                 spark_table_name
                 ):
        """
        Based on type it will load the Hudi Tables
        :param table_name: str
        :param hudi_path: str
        :param spark_session: obj
        """
        self.__table_name = table_name
        self.__hudi_path = hudi_path
        self.__type = type
        self.__spark_session = spark_session
        self.__spark_table_name = spark_table_name
        self.__check_points_bucket = check_points_bucket

    def incremental_load(self):
        """
        Creates and load the Incremental data
        :return: Bool
        """
        try:

            helper = HUDIIncrementalReader(
                bucket=self.__check_points_bucket,
                hudi_settings=HUDISettings(
                    table_name=self.__table_name,
                    path=self.__hudi_path
                ),
                spark_session=self.__spark_session
            )
            spark_df = helper.read()
            spark_df.createOrReplaceTempView(self.__spark_table_name)
            return True
        except Exception as e:
            print(f"Error loading Table {self.__table_name} {e}")
            return False

    def full_load(self):
        try:
            self.__spark_session.read.format("hudi").load(self.__hudi_path).createOrReplaceTempView(
                self.__spark_table_name)
            return True
        except Exception as e:
            print(f"Error loading Table {self.__table_name} {e}")
            return False


class Loaders(object):
    def __init__(self, json_payload, spark_session):
        self.json_payload = json_payload
        self.spark_session = spark_session

    def load(self):
        for item in self.json_payload.get("source"):
            if item.get("source_type") == "HUDI":
                helper = HudiLoader(
                    hudi_path=item.get("path"),
                    table_name=item.get("table_name"),
                    check_points_bucket=self.json_payload.get("check_point_buckets"),
                    spark_session=self.spark_session,
                    spark_table_name=item.get("spark_table_name")
                )
                if item.get("type") == "INC":
                    helper.incremental_load()

                if item.get("type") == "FULL":
                    helper.full_load()


def write_to_hudi(spark_df,
                  table_name,
                  db_name,
                  method='upsert',
                  table_type='COPY_ON_WRITE',
                  recordkey='',
                  precombine='',
                  partition_fields='',
                  folder='silver'
                  ):
    path = f"s3a://huditest/{folder}/database={db_name}/table_name={table_name}"

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


def main():
    helper = Loaders(
        json_payload=loaders_json_payload,
        spark_session=spark
    )
    helper.load()

    print("\n")
    print(spark.sql("select * from customers").show(2))
    print("\n")
    print(spark.sql("select * from orders").show(2))
    print("\n")

    query = """
SELECT  c.customer_id,
        c.name AS customer_name,
        c.email,
        o.order_id,
        o.name AS order_name,
        o.order_value
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
    """

    df_result = spark.sql(query)
    print("****************")
    df_result.show()

    write_to_hudi(
        spark_df=df_result,
        db_name="default",
        table_name="gold_orders_with_customers",
        recordkey="order_id",
        precombine="order_id",
        partition_fields="",
        method="upsert",
        folder="gold"
    )


main()
