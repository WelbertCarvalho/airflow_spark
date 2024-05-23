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

    def spark_df_using_list(self, data_list: list, schema: StructType, spark_session: SparkSession) -> DataFrame:
        '''
        This method creates a Spark Dataframe, receiving a list of records and a Spark Session.
        '''
        spark_dataframe = spark_session.createDataFrame(data_list, schema)
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
    