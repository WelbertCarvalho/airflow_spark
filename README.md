# Pyspark project using OOP and a medallion architecture

## Project structure
- Object Oriented Programming.
- This project uses PySpark 3.5 and Airflow 2.6.2.
- A virtual environment named 'venv' needs to be created exporting JAVA_HOME pointing to the default java installation in your machine.
- The same 'venv' needs to contain another variable named AIRFLOW_HOME pointing to the path of your airflow installation.
- When cofiguring your airflow.cfg, you need to reset the variable dag_folder pointing to the folder named dags in the folder instalacao_airflow.
- The classes are separated per finallity having data engineering activities.
- For viability reasons, I'm avoiding maintaining some critical files and some unnecessary files in this repo. If you have any doubts, I can explain and show the application running in a local environment.

## Structure

```
airflow/
    ├───instalacao_airflow/
    |       ├──dags/
    |       |   └──dag_extract_breweries.py
    |       └──logs/
    |           ├──dag_id=pipeline_abinbev/
    |           ├──dag_processor_manager/
    |           └──scheduler/
    ├───pipelines/
    |       └──proj_abinbev/
    |           ├───datalake/
    |           |     ├──bronze/
    |           |     ├──silver/
    |           |     └──gold/
    |           ├───extraction.py/
    |           ├───loading.py/
    |           ├───main.py/
    |           ├───manage_spark.py/
    |           ├───transformation.py
    |           └───dags/
    |               └──dag_extract_breweries.py
    ├───.gitignore/
    ├───README.md/
    └───requirements.txt
```

## Explanation:

- **datalake**: Contains all the files that compose the datalake.
- **extraction.py**: Contains the class for extract data from the API.
- **loading.py**: Contains the class to load data after transformed.
- **main.py**: Contains the instances of the classes and the method calls needed to execute the pipeline.  
- **manage_spark.py**: Contains the class and configuration to initialize and stop the spark session.
- **transformation.py**: Contains the class to execute all the necessary transformations.
- **dags**: Contains the definition of the DAGs that will be used within airflow.


## Classes

### To manage spark (manage_spark.py)
This class provides a spark session object having the option to use delta tables or not.

### Data extraction via API for testing (extraction.py)
In this class I developed a data extraction using an API called Open Brewery DB API.

### Data transformation (transformation.py)
A class created to provide some high level methods in order to transform data easier.

### A class to load data (loading.py)
The objective here is to provide methods to create delta tables or just to export the transformed data into a datalake with medallion architecture.


## Dependencies

### requirements.txt
To install all the requirements use: `pip install -r requirements.txt`


## Monitoring/Alerting
A view will be implemented to query an Airflow database table, which contains a table called 'dag_run'. In this table, we can see whether a DAG had a state of success or not. We can also look at other tables that provide substantial information about errors in DAG executions.
