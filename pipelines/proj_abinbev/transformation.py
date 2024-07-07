from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import StructType

class DataTransformer():
    def __init__(self, short_description: str, layer: str) -> None:
        self._short_description = short_description
        self._layer = layer

    def __str__(self) -> str:
        return f'Description: {self._short_description} \nLayer: {self._layer}'

    def spark_df_using_json(self, file_path: str, spark_session: SparkSession, schema: StructType = None) -> DataFrame:  
        '''
        This method creates a Spark Dataframe, receiving a json and a Spark Session.
        '''
        if schema:
            spark_dataframe = spark_session.read.schema(schema).json(file_path)
        else:
            spark_dataframe = spark_session.read.json(file_path)
        return spark_dataframe
    
    def spark_df_using_parquet(self, file_path: str, spark_session: SparkSession, schema: StructType = None) -> DataFrame:  
        '''
        This method creates a Spark Dataframe, receiving parquet files and a Spark Session.
        '''
        if schema:
            spark_dataframe = spark_session.read.schema(schema).parquet(file_path)
        else:
            spark_dataframe = spark_session.read.parquet(file_path)
        return spark_dataframe

    
    def deduplicate_data(self, spark_dataframe: DataFrame, column_name_ref: str, column_to_orderby: str) -> DataFrame:
        '''
        This method deduplicate data using an specific column, receiving an Spark DataFrame and the column name as a string.
        '''
        window_spec = (
            Window
                .partitionBy('pk', col(column_name_ref))
                .orderBy(col(column_to_orderby).desc())
        )

        spark_dataframe = (
            spark_dataframe
                .withColumn('rank', row_number().over(window_spec))
                .filter(col('rank') == 1)
                .drop('rank')
                .orderBy(col(column_to_orderby).desc())
        )

        return spark_dataframe
    

if __name__ == '__main__':
    from pyspark.sql.types import StructType, StructField, StringType

    from extraction import DataExtractor
    from manage_spark import ManageSpark

    extract_obj = DataExtractor(
        project_name = 'AbInbev project',
        project_url = 'https://api.openbrewerydb.org/breweries'
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

    bronze_dataframe.show()
    