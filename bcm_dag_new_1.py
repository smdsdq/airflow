from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import subprocess
import logging
import time
from datetime import datetime
import os
import requests

# Generate per-run log file path
run_id = os.environ.get('AIRFLOW_CTX_DAG_RUN_ID', datetime.now().strftime('%Y%m%d%H%M%S'))
LOG_FILE = f"/airflow/logs/bcm_failover_{run_id}.log"

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def log1(msg, level='info'):
    if level == 'info':
        logging.info(msg)
    elif level == 'error':
        logging.error(msg)
    print(msg)

def log(msg, level='info'):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now().isoformat()} {level.upper()} - {msg}\n")
    print(f"{level.upper()} - {msg}")

def run_ansible(playbook):
    cmd = ["ansible-playbook", "-i", "localhost,", "-c", "local", playbook]
    log(f"Running Ansible playbook: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        log(result.stdout)
    except subprocess.CalledProcessError as e:
        log(e.stderr, 'error')
        log(f"NOTIFY=Playbook {playbook} failed, notifying service delivery team")
        raise

def call_manage_service(host, service_name, action):
    cmd = [
        "python3",
        "/opt/airflow/scripts/manage_service.py",
        "--id", "7793",
        "--hostname", host,
        "--actionType", action,
        f"--serviceName={service_name}"
    ]
    log(f"Executing: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        log(result.stdout)
    except subprocess.CalledProcessError as e:
        log(e.stderr, 'error')
        log(f"NOTIFY=Service action failed on {host} for {service_name}")
        raise

def handover_wait(stage):
    log(f"Handover wait for stage: {stage}")
    time.sleep(10)
    log(f"Handover complete for stage: {stage}")

def check_local_dummy_service_status():
    # Example: systemctl is-active dummy_service
    cmd = ["systemctl", "is-active", "cron"]
    log("Checking dummy_service status on localhost")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        status = result.stdout.strip()
        log(f"dummy_service status: {status}")
        if status != 'active':
            log("dummy_service not active, attempting to start", 'error')
            start_dummy_service()
    except subprocess.CalledProcessError as e:
        log(f"Error checking dummy_service: {e.stderr}", 'error')
        raise

def start_dummy_service():
    cmd = ["systemctl", "start", "cron"]
    log("Starting dummy_service on localhost")
    try:
        subprocess.run(cmd, check=True)
        log("dummy_service started successfully")
    except subprocess.CalledProcessError as e:
        log(f"Failed to start dummy_service: {e.stderr}", 'error')
        log("NOTIFY=Failed to start dummy_service on localhost")
        raise

def stop_dummy_service():
    cmd = ["systemctl", "stop", "cron"]
    log("Stopping dummy_service on localhost")
    try:
        subprocess.run(cmd, check=True)
        log("dummy_service stopped successfully")
    except subprocess.CalledProcessError as e:
        log(f"Failed to stop dummy_service: {e.stderr}", 'error')
        log("NOTIFY=Failed to stop dummy_service on localhost")
        raise

def check_http_site(url):
    log(f"Checking site availability: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            log(f"Site {url} is up and running")
        else:
            log(f"Site {url} returned status code {resp.status_code}", 'error')
            log(f"NOTIFY=Site {url} is down or unhealthy")
    except Exception as e:
        log(f"Exception while checking site {url}: {e}", 'error')
        log(f"NOTIFY=Site {url} check failed")

def pre_checks():
    log("Starting pre-checks")
    run_ansible("/opt/airflow/ansible/bcm_prechecks.yml")
    check_local_dummy_service_status()
    check_http_site("http://example-site1.com")
    check_http_site("http://example-site2.com")

def failover():
    log("Starting failover")
    run_ansible("/opt/airflow/ansible/bcm_failover.yml")
    # Start isc and alite on their hosts
    call_manage_service("host1.domain", "isc", "serviceStart")
    call_manage_service("host2.domain", "alite", "serviceStart")
    check_local_dummy_service_status()
    check_http_site("http://example-site1.com")
    check_http_site("http://example-site2.com")

def failback():
    log("Starting failback")
    run_ansible("/opt/airflow/ansible/bcm_failback.yml")
    # Stop isc and alite on their hosts
    call_manage_service("host1.domain", "isc", "serviceStop")
    call_manage_service("host2.domain", "alite", "serviceStop")
    check_local_dummy_service_status()
    check_http_site("http://example-site1.com")
    check_http_site("http://example-site2.com")

def post_checks():
    log("Starting post-checks")
    run_ansible("/opt/airflow/ansible/bcm_postchecks.yml")
    check_local_dummy_service_status()
    check_http_site("http://example-site1.com")
    check_http_site("http://example-site2.com")

def notify_completion():
    log("NOTIFY=BCM workflow completed successfully")

def create_dag():
    with DAG(
        dag_id="bcm_failover_demo_dag",
        start_date=days_ago(1),
        schedule_interval=None,
        catchup=False,
        tags=["bcm", "failover"],
    ) as dag:

        from airflow.utils.task_group import TaskGroup

        with TaskGroup("pre_checks", tooltip="Pre-checks", ui_color="#AED6F1") as pre_checks_tg:
            pre = PythonOperator(task_id="run_prechecks", python_callable=pre_checks)
            handover_pre = PythonOperator(task_id="handover_prechecks", python_callable=handover_wait, op_args=["prechecks"])
            pre >> handover_pre

        with TaskGroup("failover", tooltip="Failover", ui_color="#A9DFBF") as failover_tg:
            fail = PythonOperator(task_id="run_failover", python_callable=failover)
            handover_fail = PythonOperator(task_id="handover_failover", python_callable=handover_wait, op_args=["failover"])
            fail >> handover_fail

        with TaskGroup("failback", tooltip="Failback", ui_color="#F5B7B1") as failback_tg:
            fb = PythonOperator(task_id="run_failback", python_callable=failback)
            handover_fb = PythonOperator(task_id="handover_failback", python_callable=handover_wait, op_args=["failback"])
            fb >> handover_fb

        with TaskGroup("post_checks", tooltip="Post-checks", ui_color="#D7BDE2") as post_checks_tg:
            post = PythonOperator(task_id="run_postchecks", python_callable=post_checks)
            handover_post = PythonOperator(task_id="handover_postchecks", python_callable=handover_wait, op_args=["postchecks"])
            post >> handover_post

        pre_checks_tg >> failover_tg >> failback_tg >> post_checks_tg

    return dag

globals()["bcm_failover_demo_dag"] = create_dag()
