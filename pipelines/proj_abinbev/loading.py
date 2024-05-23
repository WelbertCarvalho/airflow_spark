from pyspark.sql import DataFrame

class DataLoader():
    def __init__(self, path_to_save: str, table_name: str):
        self._path_to_save = path_to_save
        self._table_name = table_name

    def __str__(self) -> str:
        return f'Path to save: {self._path_to_save} \nTable name: {self._table_name}'

    def _path_to_files(self) -> str:
        '''
        This method creates a string that provides a complete path to save files.
        '''
        complete_path = f'{self._path_to_save}/{self._table_name}' 
        return complete_path
    
    def create_table(self, dataframe_to_save: DataFrame, format: str, partition: str) -> None:
        '''
        This method creates a delta table using a dataframe as a data source.
        '''
        path_and_name_of_table = self._path_to_files()

        (
            dataframe_to_save
                .write
                .format(format)
                .mode('overwrite')
                .partitionBy(partition)
                .save(path_and_name_of_table)
        )

        return None
