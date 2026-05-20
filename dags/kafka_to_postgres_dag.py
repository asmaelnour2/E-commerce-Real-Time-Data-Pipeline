from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import subprocess
import sys

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def consume_and_process_kafka():
    
    try:
        from kafka import KafkaConsumer
        import psycopg2
    except ImportError:
        print("Installing required libraries...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python-ng", "psycopg2-binary"])
        from kafka import KafkaConsumer
        import psycopg2

    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        'clickstream',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow_consumer_group',
        consumer_timeout_ms=5000
    )

    metrics = {}
    records_count = 0

    for message in consumer:
        try:
            data = json.loads(message.value.decode('utf-8'))
            event_type = data.get('event_type', 'unknown')
            metrics[event_type] = metrics.get(event_type, 0) + 1
            records_count += 1
        except Exception as e:
            print(f"Error parsing message: {e}")
            continue

    if records_count == 0:
        print("No new messages found. Skipping DB update.")
        return

    print(f"Connecting to PostgreSQL to insert {records_count} records...")
    
    conn = psycopg2.connect(
        host="postgres",
        database="analytics_db",
        user="admin",
        password="password123", # تم التعديل ليطابق الـ docker-compose الجديد
        port="5432"
    )
    cur = conn.cursor()

    for event_type, total_events in metrics.items():
        cur.execute(
            """
            INSERT INTO gold_windowed_metrics (window_start, window_end, event_type, total_events)
            VALUES (NOW() - INTERVAL '5 minutes', NOW(), %s, %s);
            """,
            (event_type, total_events)
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Database updated successfully!")


with DAG(
    'kafka_clickstream_pipeline',
    default_args=default_args,
    description='Fetch micro-batches from Kafka and upsert to Postgres',
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:

    
    kafka_task = PythonOperator(
        task_id='consume_kafka_metrics',
        python_callable=consume_and_process_kafka,
    )

    kafka_task