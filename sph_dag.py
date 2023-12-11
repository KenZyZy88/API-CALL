from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'SPH_extract_API',
    default_args=default_args,
    description='API to retreive post posted in Facebook',
    #schedule_interval='*/10 * * * *', # Run every 10 minutes
    schedule_interval='0 */6 * * *', # Test script work with actual data
    catchup=False,
)

backup = BashOperator(
    task_id='000_backup',
    bash_command='python3 /home/kenny/py/000_backup.py',
    dag=dag,
)


extract = BashOperator(
    task_id='100_extract_crowdtangle_api',
    bash_command='python3 /home/kenny/py/100_extract_crowdtangle_api.py',
    dag=dag,
)


transform_media = BashOperator(
    task_id='200_transform_ct_media.py',
    bash_command='python3 /home/kenny/py/200_transform_ct_media.py',
    dag=dag,
)

load_post = BashOperator(
    task_id='300_load_ct_post',
    bash_command='python3 /home/kenny/py/300_load_ct_post.py',
    dag=dag,
)

load_account = BashOperator(
    task_id='300_load_ct_account',
    bash_command='python3 /home/kenny/py/300_load_ct_account.py',
    dag=dag,
)

load_statistic = BashOperator(
    task_id='300_load_ct_statistic',
    bash_command='python3 /home/kenny/py/300_load_ct_statistic.py',
    dag=dag,
)

load_media = BashOperator(
    task_id='300_load_ct_media',
    bash_command='python3 /home/kenny/py/300_load_ct_media.py',
    dag=dag,
)


backup >> extract
extract >> load_post
extract >> transform_media >> load_media
extract >> load_account
extract >> load_statistic
