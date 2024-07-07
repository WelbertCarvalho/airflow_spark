import os
import sys
sys.path.append("/home/welbert/projetos/airflow")

import json
from pyspark.sql.functions import col, count

from pipelines.proj_abinbev.manage_spark import ManageSpark
from pipelines.proj_abinbev.extraction import DataExtractor
from pipelines.proj_abinbev.transformation import DataTransformer
from pipelines.proj_abinbev.loading import DataLoader


def extraction(ti, **kwargs) -> str:
    url = kwargs['url']
    project_name = kwargs['project_name']
    path_to_save = kwargs['path_to_save']
    
    extract_obj = DataExtractor(
        project_name = project_name,
        project_url = url
    )

    data = extract_obj.get_json_data()
    data_flattened = extract_obj.flatten_list(list_to_be_flattened = data['json_data'])
    tot_items = extract_obj.get_tot_items(dict_json_data = data['json_data'])
    print(f"Total pages: {data['page']} | Total Items: {tot_items}")
    print(type(data_flattened))

    file_path = os.path.join(path_to_save, 'pre_bronze.json')
    with open(file_path, 'w', encoding = 'utf-8') as json_file:
        json.dump(data_flattened, json_file)

    ti.xcom_push(key = 'pre_bronze_path', value = file_path)

    return file_path

def raw_layer(ti, **kwargs) -> str:
    path_bronze = kwargs['path_bronze']
    schema = kwargs['schema']
    table_name = kwargs['table_name']
    partition = kwargs['partition']

    manager_spark_obj = ManageSpark(
        app_name = 'Data Engineering'
    )
    spark_session = manager_spark_obj.start_spark()
    spark_session.sparkContext.setLogLevel('FATAL')
    
    data_path = ti.xcom_pull(task_ids = 'extraction_task', key = 'pre_bronze_path')

    data_transformer_obj = DataTransformer(
        short_description = 'Creating the spark dataframe for raw layer',
        layer = 'bronze'
    )

    bronze_dataframe = data_transformer_obj.spark_df_using_json(
        file_path = data_path,
        spark_session = spark_session,
        schema = schema
    )

    print(f'Dataframe: bronze_dataframe {20*"-"}')
    bronze_dataframe.show()
    bronze_dataframe.printSchema()

 
    data_loader_obj_bronze = DataLoader(
        path_to_save = path_bronze,
        table_name = table_name
    )

    data_loader_obj_bronze.create_table(
        dataframe_to_save = bronze_dataframe,
        format = 'parquet',
        partition = partition
    )

    manager_spark_obj.stop_spark(spark_session)

    print(f'Path: {data_loader_obj_bronze._path_to_save}')
    print(f'Table name: {data_loader_obj_bronze._table_name}\n')

    ti.xcom_push(key = 'bronze_path', value = data_loader_obj_bronze._path_to_save)

    return data_loader_obj_bronze._path_to_save

def silver_layer(ti, **kwargs) -> str:
    path_silver = kwargs['path_silver']
    table_name = kwargs['table_name']
    partition = kwargs['partition']

    manager_spark_obj = ManageSpark(
        app_name = 'Data Engineering'
    )
    spark_session = manager_spark_obj.start_spark()
    spark_session.sparkContext.setLogLevel('FATAL')

    bronze_path = ti.xcom_pull(task_ids = 'raw_layer_task', key = 'bronze_path')

    data_transformer_obj = DataTransformer(
        short_description = 'Creating the spark dataframe for silver layer',
        layer = 'silver'
    )

    raw_dataframe = data_transformer_obj.spark_df_using_parquet(
        file_path = bronze_path,
        spark_session = spark_session
    )

    silver_dataframe = (
        raw_dataframe
            .withColumn('longitude', col('longitude').cast('double'))
            .withColumn('latitude', col('longitude').cast('double'))
    )

    print(f'Dataframe: silver_dataframe {20*"-"}')
    silver_dataframe.show()
    silver_dataframe.printSchema()

    

    data_loader_obj_silver = DataLoader(
        path_to_save = path_silver,
        table_name = table_name
    )

    data_loader_obj_silver.create_table(
        dataframe_to_save = silver_dataframe,
        format = 'parquet',
        partition = partition
    )

    manager_spark_obj.stop_spark(spark_session)

    print(f'Path: {data_loader_obj_silver._path_to_save}')
    print(f'Table name: {data_loader_obj_silver._table_name}\n')

    ti.xcom_push(key = 'silver_path', value = data_loader_obj_silver._path_to_save)

    return data_loader_obj_silver._path_to_save

def gold_layer(ti, **kwargs) -> str:
    path_gold = kwargs['path_gold'] 
    groupby_2 = kwargs['groupby_2']
    table_name = kwargs['table_name']
    partition = kwargs['partition']
    
    manager_spark_obj = ManageSpark(
        app_name = 'Data Engineering'
    )
    spark_session = manager_spark_obj.start_spark()
    spark_session.sparkContext.setLogLevel('FATAL')



    silver_path = ti.xcom_pull(task_ids = 'silver_layer_task', key = 'silver_path')

    data_transformer_obj = DataTransformer(
        short_description = 'Creating the spark dataframe for gold layer',
        layer = 'gold'
    )

    silver_dataframe = data_transformer_obj.spark_df_using_parquet(
        file_path = silver_path,
        spark_session = spark_session
    )

    gold_dataframe = (
        silver_dataframe
            .groupBy(
                groupby_2[0],  
                groupby_2[1]   
            )
            .agg(count(groupby_2[0]).alias('qty_brewery_type_location'))
            .orderBy(col(groupby_2[0]).asc(), col(groupby_2[1]).asc())
    )

    print(f'Dataframe: gold_dataframe {20*"-"}')
    gold_dataframe.show()
    gold_dataframe.printSchema()

    data_loader_obj_gold = DataLoader(
        path_to_save = path_gold,
        table_name = table_name
    )

    data_loader_obj_gold.create_table(
        dataframe_to_save = gold_dataframe,
        format = 'parquet',
        partition = partition
    )

    print(f'Path: {data_loader_obj_gold._path_to_save}')
    print(f'Table name: {data_loader_obj_gold._table_name}\n')

    gold_dataframe.createOrReplaceTempView('gold_dataframe')

    vw_breweries = (
        spark_session
            .sql('select * from gold_dataframe')
    )

    print(f'View: vw_breweries {20*"-"}')
    vw_breweries.show()
    vw_breweries.printSchema()

    manager_spark_obj.stop_spark(spark_session)

    return data_loader_obj_gold._path_to_save
