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
    'SPH_extract_leaderboard_API',
    default_args=default_args,
    description='API to retreive leadersboard in Facebook',
    #schedule_interval='*/10 * * * *', # Run every 10 minutes
    schedule_interval='0 */6 * * *', # Test script work with actual data
    catchup=False,
)

backup = BashOperator(
    task_id='000_backup',
    bash_command='python3 /home/kenny/py/leaderboard/000_backup.py',
    dag=dag,
)


extract = BashOperator(
    task_id='100_extract_leaderboard',
    bash_command='python3 /home/kenny/py/leaderboard/100_extract_leaderboard.py',
    dag=dag,
)


transform_leaderboard_acount = BashOperator(
    task_id='200_transform_ct_leaderboard_account',
    bash_command='python3 /home/kenny/py/leaderboard/200_transform_ct_leaderboard_account.py',
    dag=dag,
)

transform_leaderboard_summary = BashOperator(
    task_id='200_transform_ct_leaderboard_summary',
    bash_command='python3 /home/kenny/py/leaderboard/200_transform_ct_leaderboard_summary.py',
    dag=dag,
)

transform_leaderboardbreakdown = BashOperator(
    task_id='200_transform_ct_leaderboardbreakdown',
    bash_command='python3 /home/kenny/py/leaderboard/200_transform_ct_leaderboardbreakdown.py',
    dag=dag,
)

load_leaderboard_account = BashOperator(
    task_id='300_load_ct_leaderboard_account',
    bash_command='python3 /home/kenny/py/leaderboard/300_load_ct_leaderboard_account.py',
    dag=dag,
)

load_leaderboard_breakdown = BashOperator(
    task_id='300_load_ct_leaderboard_breakdown',
    bash_command='python3 /home/kenny/py/leaderboard/300_load_ct_leaderboard_breakdown.py',
    dag=dag,
)

load_leaderboard_summary = BashOperator(
    task_id='300_load_ct_leaderboard_summary.py',
    bash_command='python3 /home/kenny/py/leaderboard/300_load_ct_leaderboard_summary.py',
    dag=dag,
)

load_subscriberData_table = BashOperator(
    task_id='300_load_ct_subscriberData_table',
    bash_command='python3 /home/kenny/py/leaderboard/300_load_ct_subscriberData_table.py',
    dag=dag,
)


backup >> extract
extract >> transform_leaderboard_acount >> load_leaderboard_account
extract >> transform_leaderboard_summary >> load_leaderboard_summary
extract >> transform_leaderboardbreakdown >> load_leaderboard_breakdown
extract >> load_subscriberData_table