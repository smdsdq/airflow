import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

# Configuration variables
ANSIBLE_PLAYBOOK_PATH = "/app/application/ansible/service_check.yml"
PYTHON_SCRIPT_PATH = "/app/application/scripts/failover_script.py"
SERVICE_LIST = ["service1", "service2"]  # Replace with actual services
HOSTNAME = "localhost"
FAILOVER_ID = "12345"
ACTION_TYPE_FAILOVER = "failover"
ACTION_TYPE_FAILBACK = "failback"

def run_ansible_playbook(task_type):
    """Execute Ansible playbook with error handling"""
    try:
        logger.info(f"Starting {task_type} check for services: {SERVICE_LIST} on {HOSTNAME}")
        # Construct Ansible command
        service_list_str = ",".join(SERVICE_LIST)
        ansible_cmd = f"ansible-playbook {ANSIBLE_PLAYBOOK_PATH} -e 'services={service_list_str} hostname={HOSTNAME}'"
        
        # Execute command (handled by BashOperator)
        return ansible_cmd
    except Exception as e:
        logger.error(f"Error in {task_type} check: {str(e)}")
        raise AirflowException(f"{task_type} check failed: {str(e)}")

def run_python_script(id, action_type, service_name, hostname):
    """Execute Python script with error handling"""
    try:
        logger.info(f"Starting {action_type} for service: {service_name} on {hostname}")
        # Construct Python command
        python_cmd = (
            f"python {PYTHON_SCRIPT_PATH} "
            f"--id {id} "
            f"--actionType {action_type} "
            f"--serviceName {service_name} "
            f"--hostname {hostname}"
        )
        return python_cmd
    except Exception as e:
        logger.error(f"Error in {action_type}: {str(e)}")
        raise AirflowException(f"{action_type} failed: {str(e)}")

# Define the DAG
with DAG(
    'service_failover_dag',
    default_args=default_args,
    description='DAG for service failover and failback with checks',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Precheck task
    precheck = BashOperator(
        task_id='precheck',
        bash_command=run_ansible_playbook('precheck'),
    )

    # Delay after precheck
    delay1 = DummyOperator(
        task_id='delay1',
        trigger_rule='all_success',
    )

    # Failover tasks (one per service)
    failover_tasks = []
    for service in SERVICE_LIST:
        failover = BashOperator(
            task_id=f'failover_{service}',
            bash_command=run_python_script(FAILOVER_ID, ACTION_TYPE_FAILOVER, service, HOSTNAME),
        )
        failover_tasks.append(failover)

    # Delay after failover
    delay2 = DummyOperator(
        task_id='delay2',
        trigger_rule='all_success',
    )

    # Failback tasks (one per service)
    failback_tasks = []
    for service in SERVICE_LIST:
        failback = BashOperator(
            task_id=f'failback_{service}',
            bash_command=run_python_script(FAILOVER_ID, ACTION_TYPE_FAILBACK, service, HOSTNAME),
        )
        failback_tasks.append(failback)

    # Delay after failback
    delay3 = DummyOperator(
        task_id='delay3',
        trigger_rule='all_success',
    )

    # Postcheck task
    postcheck = BashOperator(
        task_id='postcheck',
        bash_command=run_ansible_playbook('postcheck'),
    )

    # Set task dependencies with delays
    precheck >> delay1
    delay1.set_downstream(failover_tasks)
    for failover in failover_tasks:
        failover >> delay2
    delay2.set_downstream(failback_tasks)
    for failback in failback_tasks:
        failback >> delay3
    delay3 >> postcheck

    # Add delay between tasks (2 minutes each)
    for delay in [delay1, delay2, delay3]:
        delay.execution_timeout = datetime.timedelta(minutes=2)