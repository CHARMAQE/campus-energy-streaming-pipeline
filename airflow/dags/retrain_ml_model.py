from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'energy_monitoring',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 20),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def validate_model_accuracy():
    """Check if new model is better than current one"""
    with open('/opt/airflow/spark/models/random_forest_energy/metadata.json', 'r') as f:
        metadata = json.load(f)
    
    accuracy = metadata['accuracy']
    print(f"New model accuracy: {accuracy}")
    
    if accuracy < 0.90:  # Minimum 90% accuracy
        raise ValueError(f"Model accuracy {accuracy} below threshold!")
    
    return accuracy

dag = DAG(
    'daily_ml_retrain',
    default_args=default_args,
    description='Retrain Random Forest model with latest data',
    schedule_interval='0 2 * * 0',  # Every Sunday at 2 AM
    catchup=False,
    tags=['ml', 'training'],
)

# Task 1: Generate fresh training data
generate_data = BashOperator(
    task_id='generate_training_data',
    bash_command='python /opt/airflow/spark/data_generator_training.py',
    dag=dag,
)

# Task 2: Train new model
train_model = BashOperator(
    task_id='train_random_forest',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] \
      /opt/spark/work-dir/train_random_forest.py
    """,
    dag=dag,
)

# Task 3: Validate model performance
validate = PythonOperator(
    task_id='validate_accuracy',
    python_callable=validate_model_accuracy,
    dag=dag,
)

# Task 4: Send notification
notify = BashOperator(
    task_id='send_notification',
    bash_command='echo "✅ ML model retrained successfully!" | mail -s "Model Update" charmaqe4@gmail.com',
    dag=dag,
)

# Define task dependencies
generate_data >> train_model >> validate >> notify