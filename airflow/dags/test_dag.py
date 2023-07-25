from datetime import timedelta
import os


from airflow import DAG
from airflow.utils.decorators import apply_defaults

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from fairifier.rdf import upload_triples_dir, upload_terminology, rdf_conversion
from fairifier.util import setup_tmp_dir, remove_tmp_dir, GitCloneOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
        'test_rdf',
        default_args=default_args,
        description='Generate a triples output file',
        schedule_interval=None,
        start_date=days_ago(0),
        catchup=False,
        max_active_runs=1
) as dag:
    setup_op = PythonOperator(
        task_id='initialize',
        python_callable=setup_tmp_dir,
        provide_context=True
    )
    fetch_r2rml_op = GitCloneOperator(
        task_id='get_r2rml_files',
        repo_name='r2rml_files_git',
        repo_url=Variable.get('R2RML_REPO'),
        target_dir='{{ ti.xcom_pull(key="working_dir", task_ids="initialize") }}/ttl',
        repo_path='{{ ti.xcom_pull(key="working_dir", task_ids="initialize") }}/gitrepos/ttl',
        sub_dir=Variable.get('R2RML_REPO_SUBDIR'),
    )

    generate_triples_op = PythonOperator(
        task_id="rdf_conversion",
        python_callable=rdf_conversion,

        op_kwargs={'workdir': "{{ ti.xcom_pull(key='working_dir', task_ids='initialize') }}",
                   "r2rml_cli_dir": Variable.get('R2RML_CLI_DIR'),
                   "rdb_connstr": Variable.get('R2RML_RDB_CONNSTR'),
                   "rdb_user": Variable.get('R2RML_RDB_USER'),
                   "rdb_pass": Variable.get('R2RML_RDB_PASSWORD')
                   }
    )

    setup_op >> fetch_r2rml_op >> generate_triples_op
