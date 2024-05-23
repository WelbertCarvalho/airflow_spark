from datetime import datetime
from pyspark.sql import SparkSession

class ManageSpark():
    def __init__(self, app_name: str):
        self._started_in = datetime.now().strftime('%y-%m-%d %H:%M:%S')
        self._app_name = app_name
    
    def __str__(self) -> str:
        return f'App name: {self._app_name} \nStarted in: {self._started_in}'

    def start_spark(self) -> SparkSession:
        '''
        This method starts a new Spark Session.
        '''
        spark = (
            SparkSession
                .builder
                .master('local[*]')
                .appName(self._app_name)
                .getOrCreate()
        )

        return spark

    def stop_spark(self, spark_session: SparkSession) -> None:
        '''
        This method stops an existing Spark Session, taking as a parameter the variable that contains the Spark Session.
        '''
        spark_session.stop()
        print('------- The spark session was ended -------')
        return None
