import sys
sys.path.append("/home/welbert/projetos/airflow")

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql.types import StructType, StructField, StringType
import pendulum

from pipelines.proj_abinbev.main import extraction, raw_layer, silver_layer, gold_layer

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True)
    ])


groupby_2 = ['brewery_type', 'state']

default_args = {
    'owner':'airflow',
    'start_date': pendulum.datetime(2024, 7, 1, tz = "America/Sao_Paulo")
}

dag = DAG(
    'pipeline_abinbev',
    default_args = default_args,
    schedule_interval = '0 15 * * *',
    catchup = False
)

extraction_task = PythonOperator(
    task_id = 'extraction_task',
    python_callable = extraction,
    provide_context = True,
    op_kwargs = {
        'url':'https://api.openbrewerydb.org/breweries',
        'project_name':'AbInbev project',
        'path_to_save':'/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/pre_bronze'
    },
    dag = dag
)

raw_layer_task = PythonOperator(
    task_id = 'raw_layer_task',
    python_callable = raw_layer,
    provide_context = True,
    op_kwargs = {
        'path_bronze':'/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/bronze',
        'schema': schema,
        'table_name':'breweries',
        'partition':'brewery_type'
    },
    dag = dag
)

silver_layer_task = PythonOperator(
    task_id = 'silver_layer_task',
    python_callable = silver_layer,
    provide_context = True,
    op_kwargs = {
        'path_silver':'/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/silver',
        'table_name':'breweries',
        'partition':'state'
    },
    dag = dag
)

gold_layer_task = PythonOperator(
    task_id = 'gold_layer_task',
    python_callable = gold_layer,
    provide_context = True,
    op_kwargs = {
        'path_gold':'/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/gold',
        'groupby_2': groupby_2,
        'table_name':'breweries',
        'partition':'state'
    },
    dag = dag
)

extraction_task >> raw_layer_task >> silver_layer_task >> gold_layer_task