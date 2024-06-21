import sys
sys.path.append("/home/welbert/projetos/airflow")

from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pendulum

from pipelines.proj_abinbev.main import pipeline_abinbev

with DAG(
    'pipeline_abinbev',
    start_date = pendulum.datetime(2024, 6, 7, tz = "America/Sao_Paulo"),
    schedule_interval = '0 15 * * *',
    catchup = False
) as dag:

    pipeline = PythonOperator(
        task_id = 'extrai_dados_api',
        python_callable = pipeline_abinbev,
        op_kwargs = {
            'url':'https://api.openbrewerydb.org/breweries',
            'path_bronze':'/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/bronze', 
            'path_silver': '/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/silver', 
            'path_gold': '/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/gold'
        }
    )


pipeline