import datetime
import logging
import subprocess
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.models import Variable
import time

# Configure verbose logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=2),
}

# Configuration variables
ANSIBLE_PLAYBOOK_PATH = "/app/application/ansible/service_check.yml"
PYTHON_SCRIPT_PATH = "/app/application/scripts/failover_script.py"
CONFIG_PATH = "/app/config/services.json"
HOSTNAME = "localhost"
FAILOVER_ID = "12345"
ACTION_TYPE_FAILOVER = "failover"
ACTION_TYPE_FAILBACK = "failback"
ACTION_TYPE_ROLLBACK = "rollback"
DELAY_MINUTES = float(Variable.get("failover_delay_minutes", default=2))

def get_service_list(config_path=CONFIG_PATH):
    """Fetch service list from configuration file"""
    logger.debug("Fetching service list from %s", config_path)
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            services = config.get('services', [])
            logger.info("Retrieved services: %s", services)
            if not services:
                raise AirflowException("No services found in configuration")
            return services
    except Exception as e:
        logger.error("Failed to fetch service list: %s", str(e), exc_info=True)
        raise AirflowException(f"Service list retrieval failed: {str(e)}")

def update_service_state(service, state):
    """Update service state in Airflow Variables"""
    logger.debug("Updating state for %s to %s", service, state)
    try:
        Variable.set(f"service_state_{service}", state)
        logger.info("State updated for %s: %s", service, state)
    except Exception as e:
        logger.error("Failed to update state for %s: %s", service, str(e), exc_info=True)
        raise AirflowException(f"State update failed for {service}")

def run_ansible_playbook(task_type):
    """Execute Ansible playbook with verbose logging"""
    try:
        logger.debug(f"Preparing {task_type} check for hostname: {HOSTNAME}")
        service_list = get_service_list()
        service_list_str = ",".join([service['name'] for service in service_list])
        ansible_cmd = f"ansible-playbook {ANSIBLE_PLAYBOOK_PATH} -e 'services={service_list_str} hostname={HOSTNAME}' -vvv"
        
        logger.info(f"Executing Ansible command: {ansible_cmd}")
        process = subprocess.run(
            ansible_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.debug(f"{task_type} stdout: {process.stdout}")
        logger.debug(f"{task_type} stderr: {process.stderr}")
        logger.info(f"{task_type} check completed successfully")
        return process.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"{task_type} check failed with return code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise AirflowException(f"{task_type} check failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in {task_type} check: {str(e)}", exc_info=True)
        raise AirflowException(f"{task_type} check failed: {str(e)}")

def simulate_failure(service):
    """Simulate service failure for chaos testing"""
    logger.debug("Simulating failure for %s on %s", service, HOSTNAME)
    try:
        cmd = f"ansible {HOSTNAME} -m service -a 'name={service} state=stopped'"
        logger.info(f"Executing chaos command: {cmd}")
        process = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.debug("Chaos stdout: %s", process.stdout)
        logger.debug("Chaos stderr: %s", process.stderr)
        logger.info("Chaos test completed for %s", service)
        update_service_state(service, "stopped")
    except subprocess.CalledProcessError as e:
        logger.error(f"Chaos test failed for {service} with return code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise AirflowException(f"Chaos test failed for {service}")
    except Exception as e:
        logger.error(f"Unexpected error in chaos test for {service}: {str(e)}", exc_info=True)
        raise AirflowException(f"Chaos test failed for {service}")

def run_python_script(id, action_type, service_name, hostname):
    """Execute Python script with verbose logging and idempotency"""
    try:
        current_state = Variable.get(f"service_state_{service_name}", default="unknown")
        logger.debug("Current state for %s: %s", service_name, current_state)
        
        if action_type == ACTION_TYPE_FAILOVER and current_state == "failed_over":
            logger.info("Skipping failover for %s: already failed over", service_name)
            return
        if action_type == ACTION_TYPE_FAILBACK and current_state == "active":
            logger.info("Skipping failback for %s: already active", service_name)
            return
        if action_type == ACTION_TYPE_ROLLBACK and current_state != "failed_over":
            logger.info("Skipping rollback for %s: not in failed over state", service_name)
            return

        logger.debug(f"Preparing {action_type} for service: {service_name} on {hostname} with ID: {id}")
        python_cmd = (
            f"python {PYTHON_SCRIPT_PATH} "
            f"--id {id} "
            f"--actionType {action_type} "
            f"--serviceName {service_name} "
            f"--hostname {hostname}"
        )
        logger.info(f"Executing Python command: {python_cmd}")
        process = subprocess.run(
            python_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.debug(f"{action_type} stdout: {process.stdout}")
        logger.debug(f"{action_type} stderr: {process.stderr}")
        logger.info(f"{action_type} for {service_name} completed successfully")
        
        # Update state based on action
        new_state = "failed_over" if action_type == ACTION_TYPE_FAILOVER else \
                   "active" if action_type == ACTION_TYPE_FAILBACK else \
                   "rolled_back"
        update_service_state(service_name, new_state)
        return process.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"{action_type} for {service_name} failed with return code {e.returncode}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise AirflowException(f"{action_type} for {service_name} failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in {action_type} for {service_name}: {str(e)}", exc_info=True)
        raise AirflowException(f"{action_type} for {service_name} failed: {str(e)}")

def create_slack_alert(task_id, message):
    """Create a Slack alert operator"""
    return SimpleHttpOperator(
        task_id=f'slack_alert_{task_id}',
        http_conn_id='slack_webhook',
        endpoint='/',
        data=json.dumps({"text": message}),
        headers={"Content-Type": "application/json"},
        trigger_rule='one_failed',
    )

# Define the DAG
with DAG(
    'service_failover_dag',
    default_args=default_args,
    description='DAG for service failover and failback with checks and chaos testing',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Chaos testing tasks
    chaos_tasks = []
    for service in get_service_list():
        chaos = PythonOperator(
            task_id=f'chaos_{service["name"]}',
            python_callable=simulate_failure,
            op_kwargs={'service': service["name"]},
            execution_timeout=datetime.timedelta(minutes=5),
        )
        chaos_tasks.append(chaos)

    # Precheck task
    precheck = PythonOperator(
        task_id='precheck',
        python_callable=run_ansible_playbook,
        op_kwargs={'task_type': 'precheck'},
        execution_timeout=datetime.timedelta(minutes=5),
    )

    # Precheck alert
    precheck_alert = create_slack_alert(
        'precheck',
        "Precheck failed for services on {{ ds }} at {{ ts }}",
    )

    # Delay after precheck
    delay1 = DummyOperator(
        task_id='delay1',
        trigger_rule='all_success',
        execution_timeout=datetime.timedelta(minutes=DELAY_MINUTES),
    )

    # Failover tasks
    failover_tasks = []
    failover_alerts = []
    for service in get_service_list():
        failover = PythonOperator(
            task_id=f'failover_{service["name"]}',
            python_callable=run_python_script,
            op_kwargs={
                'id': FAILOVER_ID,
                'action_type': ACTION_TYPE_FAILOVER,
                'service_name': service["name"],
                'hostname': HOSTNAME
            },
            execution_timeout=datetime.timedelta(minutes=10),
        )
        failover_alert = create_slack_alert(
            f'failover_{service["name"]}',
            f"Failover failed for {service['name']} on {{ ds }} at {{ ts }}",
        )
        failover_tasks.append(failover)
        failover_alerts.append(failover_alert)
        failover >> failover_alert

    # Delay after failover
    delay2 = DummyOperator(
        task_id='delay2',
        trigger_rule='all_success',
        execution_timeout=datetime.timedelta(minutes=DELAY_MINUTES),
    )

    # Failback tasks
    failback_tasks = []
    failback_alerts = []
    rollback_tasks = []
    for service in get_service_list():
        failback = PythonOperator(
            task_id=f'failback_{service["name"]}',
            python_callable=run_python_script,
            op_kwargs={
                'id': FAILOVER_ID,
                'action_type': ACTION_TYPE_FAILBACK,
                'service_name': service["name"],
                'hostname': HOSTNAME
            },
            execution_timeout=datetime.timedelta(minutes=10),
        )
        failback_alert = create_slack_alert(
            f'failback_{service["name"]}',
            f"Failback failed for {service['name']} on {{ ds }} at {{ ts }}",
        )
        rollback = PythonOperator(
            task_id=f'rollback_{service["name"]}',
            python_callable=run_python_script,
            op_kwargs={
                'id': FAILOVER_ID,
                'action_type': ACTION_TYPE_ROLLBACK,
                'service_name': service["name"],
                'hostname': HOSTNAME
            },
            trigger_rule='one_failed',
            execution_timeout=datetime.timedelta(minutes=10),
        )
        failback_tasks.append(failback)
        failback_alerts.append(failback_alert)
        rollback_tasks.append(rollback)
        failback >> [failback_alert, rollback]

    # Delay after failback
    delay3 = DummyOperator(
        task_id='delay3',
        trigger_rule='all_success',
        execution_timeout=datetime.timedelta(minutes=DELAY_MINUTES),
    )

    # Postcheck task
    postcheck = PythonOperator(
        task_id='postcheck',
        python_callable=run_ansible_playbook,
        op_kwargs={'task_type': 'postcheck'},
        execution_timeout=datetime.timedelta(minutes=5),
    )

    # Postcheck alert
    postcheck_alert = create_slack_alert(
        'postcheck',
        "Postcheck failed for services on {{ ds }} at {{ ts }}",
    )

    # Set task dependencies
    for chaos in chaos_tasks:
        chaos >> precheck
    precheck >> precheck_alert >> delay1
    delay1.set_downstream(failover_tasks)
    for failover, failover_alert in zip(failover_tasks, failover_alerts):
        failover >> failover_alert >> delay2
    delay2.set_downstream(failback_tasks)
    for failback, failback_alert, rollback in zip(failback_tasks, failback_alerts, rollback_tasks):
        failback >> [failback_alert, rollback] >> delay3
    delay3 >> postcheck >> postcheck_alert