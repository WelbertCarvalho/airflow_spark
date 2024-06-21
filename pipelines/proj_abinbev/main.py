import sys
sys.path.append("/home/welbert/projetos/airflow")

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, count

from pipelines.proj_abinbev.manage_spark import ManageSpark
from pipelines.proj_abinbev.extraction import DataExtractor
from pipelines.proj_abinbev.transformation import DataTransformer
from pipelines.proj_abinbev.loading import DataLoader


def pipeline_abinbev(url: str, path_bronze: str, path_silver: str, path_gold: str) -> None:

    extract_obj = DataExtractor(
        project_name = 'AbInbev project',
        project_url = url
    )

    data = extract_obj.get_json_data()

    data_flattened = extract_obj.flatten_list(list_to_be_flattened = data['json_data'])
    tot_items = extract_obj.get_tot_items(dict_json_data = data['json_data'])

    print(f"Total pages: {data['page']} | Total Items: {tot_items}")

    print('Initializing transformation ---------------------------------------------------------')

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

    manager_spark_obj = ManageSpark(
        app_name = 'Data Engineering'
    )

    print(manager_spark_obj)
    spark_session = manager_spark_obj.start_spark()

    spark_session.sparkContext.setLogLevel('FATAL')

    data_transformer_obj = DataTransformer(
        short_description = 'Creating the spark dataframe for raw layer',
        layer = 'bronze'
    )

    bronze_dataframe = data_transformer_obj.spark_df_using_list(
        data_list = data_flattened,
        schema = schema,
        spark_session = spark_session
    )

    print(f'Dataframe: bronze_dataframe {20*"-"}')


    data_loader_obj_bronze = DataLoader(
        path_to_save = path_bronze,
        table_name = 'breweries'
    )

    data_loader_obj_bronze.create_table(
        dataframe_to_save = bronze_dataframe,
        format = 'parquet',
        partition = 'brewery_type'
    )

    print(f'Path: {data_loader_obj_bronze._path_to_save}')
    print(f'Table name: {data_loader_obj_bronze._table_name}\n')

    silver_dataframe = (
        bronze_dataframe
            .withColumn('longitude', col('longitude').cast('double'))
            .withColumn('latitude', col('latitude').cast('double'))
    )

    print(f'Dataframe: silver_dataframe {20*"-"}')

    data_loader_obj_silver = DataLoader(
        path_to_save = path_silver,
        table_name = 'breweries'
    )

    data_loader_obj_silver.create_table(
        dataframe_to_save = silver_dataframe,
        format = 'parquet',
        partition = 'state'
    )

    print(f'Path: {data_loader_obj_silver._path_to_save}')
    print(f'Table name: {data_loader_obj_silver._table_name}\n')

    gold_dataframe = (
        silver_dataframe
            .groupBy(
                'brewery_type',  
                'state'   
            )
            .agg(count('brewery_type').alias('qtd_brewery_type_location'))
            .orderBy(col('brewery_type').asc(), col('state').asc())
    )

    print(f'Dataframe: gold_dataframe {20*"-"}')

    data_loader_obj_gold = DataLoader(
        path_to_save = path_gold,
        table_name = 'breweries'
    )

    data_loader_obj_gold.create_table(
        dataframe_to_save = gold_dataframe,
        format = 'parquet',
        partition = 'state'
    )

    print(f'Path: {data_loader_obj_gold._path_to_save}')
    print(f'Table name: {data_loader_obj_gold._table_name}\n')

    gold_dataframe.createOrReplaceTempView('gold_dataframe')

    vw_breweries = (
        spark_session
            .sql('select * from gold_dataframe')
    )

    vw_breweries.show()

    manager_spark_obj.stop_spark(spark_session)

    return None

if __name__ == '__main__':
    pipeline_abinbev(
        url = 'https://api.openbrewerydb.org/breweries',
        path_bronze ='/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/bronze', 
        path_silver = '/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/silver', 
        path_gold = '/home/welbert/projetos/airflow/pipelines/proj_abinbev/datalake/gold'
    )