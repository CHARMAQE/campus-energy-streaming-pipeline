from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'energy_monitoring',
    'start_date': datetime(2025, 12, 20),
    'retries': 1,
}

dag = DAG(
    'daily_db_cleanup',
    default_args=default_args,
    description='Clean old data from PostgreSQL',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    catchup=False,
    tags=['maintenance', 'database'],
)

# Delete old aggregations (>30 days)
cleanup_aggregations = PostgresOperator(
    task_id='delete_old_aggregations',
    postgres_conn_id='postgres_energy',  # Configure in Airflow UI
    sql="""
        DELETE FROM aggregations 
        WHERE window_start < NOW() - INTERVAL '30 days';
        
        DELETE FROM aggregations_floor 
        WHERE window_start < NOW() - INTERVAL '30 days';
    """,
    dag=dag,
)

# Delete old anomalies (>90 days)
cleanup_anomalies = PostgresOperator(
    task_id='delete_old_anomalies',
    postgres_conn_id='postgres_energy',
    sql="""
        DELETE FROM anomalies 
        WHERE timestamp < NOW() - INTERVAL '90 days';
    """,
    dag=dag,
)

# Vacuum database
vacuum_db = PostgresOperator(
    task_id='vacuum_database',
    postgres_conn_id='postgres_energy',
    sql="VACUUM ANALYZE;",
    dag=dag,
)

def log_cleanup_stats():
    import psycopg2
    conn = psycopg2.connect(
        host='postgres',
        database='energy_monitoring',
        user='admin',
        password='admin123'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM aggregations")
    count = cursor.fetchone()[0]
    print(f"📊 Remaining aggregations: {count}")
    conn.close()

log_stats = PythonOperator(
    task_id='log_statistics',
    python_callable=log_cleanup_stats,
    dag=dag,
)

# Task dependencies
[cleanup_aggregations, cleanup_anomalies] >> vacuum_db >> log_stats