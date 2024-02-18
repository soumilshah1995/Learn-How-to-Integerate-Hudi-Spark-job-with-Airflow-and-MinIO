try:
    import sys, random, uuid
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession

    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *

    from pyspark.sql.types import *
    from datetime import datetime, date
    import boto3
    from functools import reduce
    from pyspark.sql import Row
    from faker import Faker
except Exception as e:
    print("Modules are missing : {} ".format(e))

job_start_ts = datetime.now()
ts_format = '%Y-%m-%d %H:%M:%S'
minio_endpoint = 'http://minio:9000/'
minio_access_key = 'admin'
minio_secret_key = 'password'
minio_secure = False


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




global faker
faker = Faker()


def get_customer_data(total_customers=2):
    customers_array = []
    for i in range(0, total_customers):
        customer_data = {
            "customer_id": str(uuid.uuid4()),
            "name": faker.name(),
            "state": faker.state(),
            "city": faker.city(),
            "email": faker.email(),
            "created_at": datetime.now().isoformat().__str__(),
            "address": faker.address(),

        }
        customers_array.append(customer_data)
    return customers_array


def get_orders_data(customer_ids, order_data_sample_size=3):
    orders_array = []
    for i in range(0, order_data_sample_size):
        try:
            order_id = uuid.uuid4().__str__()
            customer_id = random.choice(customer_ids)
            order_data = {
                "order_id": order_id,
                "name": faker.text(max_nb_chars=20),
                "order_value": random.randint(10, 1000).__str__(),
                "priority": random.choice(["LOW", "MEDIUM", "HIGH"]),
                "order_date": faker.date_between(start_date='-30d', end_date='today').strftime('%Y-%m-%d'),
                "customer_id": customer_id,

            }
            orders_array.append(order_data)
        except Exception as e:
            print(e)
    return orders_array


global total_customers, order_data_sample_size

total_customers = 50
order_data_sample_size = 100


def upsert_hudi_table(glue_database, table_name, record_id, precomb_key, table_type, spark_df, partition_fields,
                      enable_partition, enable_cleaner, enable_hive_sync, enable_clustering,
                      enable_meta_data_indexing,
                      use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert', clustering_column='default'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        partition_fields this is used to parrtition data
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    hudi_clustering = {
        "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
        "hoodie.clustering.inline": "true",
        "hoodie.clustering.plan.strategy.sort.columns": clustering_column,
        "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",
        "hoodie.clustering.plan.strategy.small.file.limit": "629145600"
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    # Define a dictionary with the Fiel Size
    hudi_file_size = {
        "hoodie.parquet.max.file.size": 512 * 1024 * 1024,  # 512MB
        "hoodie.parquet.small.file.limit": 104857600,  # 100MB
    }

    hudi_meta_data_indexing = {
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.async": "true",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.check.timeout.seconds": "60",
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider"
    }

    if enable_meta_data_indexing == True or enable_meta_data_indexing == "True" or enable_meta_data_indexing == "true":
        for key, value in hudi_meta_data_indexing.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    if enable_clustering == True or enable_clustering == "True" or enable_clustering == "true":
        for key, value in hudi_clustering.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    for key, value in hudi_file_size.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


# ---------------------------- CUSTOMERS ------------------------------------------------
customer_data = get_customer_data(total_customers=total_customers)

order_data = get_orders_data(
    order_data_sample_size=order_data_sample_size,
    customer_ids=[i.get("customer_id") for i in customer_data]
)

spark_df_customers = spark.createDataFrame(data=[tuple(i.values()) for i in customer_data],
                                           schema=list(customer_data[0].keys()))
spark_df_customers.show()
spark_df_orders = spark.createDataFrame(data=[tuple(i.values()) for i in order_data], schema=list(order_data[0].keys()))
spark_df_orders.show()

upsert_hudi_table(
    glue_database="hudidb",
    table_name="customers",
    record_id="customer_id",
    precomb_key="created_at",
    table_type='COPY_ON_WRITE',
    partition_fields="state",
    method='upsert',
    index_type='BLOOM',
    enable_partition=True,
    enable_cleaner=True,
    enable_hive_sync=False,
    enable_clustering='False',
    clustering_column='default',
    enable_meta_data_indexing='false',
    use_sql_transformer=False,
    sql_transformer_query='default',
    target_path="s3a://huditest/silver/table_name=customers/",
    spark_df=spark_df_customers,
)

upsert_hudi_table(
    glue_database="hudidb",
    table_name="orders",
    record_id="order_id",
    precomb_key="order_date",
    table_type='COPY_ON_WRITE',
    partition_fields="default",
    method='upsert',
    index_type='BLOOM',
    enable_partition=False,
    enable_cleaner=True,
    enable_hive_sync=False,
    enable_clustering='False',
    clustering_column='default',
    enable_meta_data_indexing='false',
    use_sql_transformer=False,
    sql_transformer_query='default',
    target_path="s3a://huditest/silver/table_name=orders/",
    spark_df=spark_df_orders,
)