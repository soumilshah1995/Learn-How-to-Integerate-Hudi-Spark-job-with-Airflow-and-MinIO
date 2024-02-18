import os
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG(
    dag_id="dag_incremental_etl",
    default_args={
        "owner": "Soumil Shah",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/incremental_etl_orders.py",
    packages="org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2",
    conf={
        "spark.driver.memory": "1g",  # Adjust as per your requirement
        "spark.executor.memory": "1g",  # Adjust as per your requirement
        "spark.executor.instances": "1"  # Adjust as per your requirement
    },
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> end
