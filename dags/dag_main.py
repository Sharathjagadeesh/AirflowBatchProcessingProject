# Airflow imports
from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.bash import BashOperator


# Module imports
from datetime import timedelta, datetime
import pendulum
import logging
logging.basicConfig(level = logging.INFO)

#### Other imports
import sys
import os

# Dynamically add the parent directory of the DAG file to sys.path
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(DAGS_FOLDER, ".."))
sys.path.insert(0, BASE_DIR)

####

# File imports
from python_import_files.api_files.tmdb_api import get_tmdb_api
from python_import_files.api_files.imdb_api import get_imdb_api
from apache_beam_pipeline.beam_imdb_clean_pipeline import beam_imdb_clean_pipeline_fn
from apache_beam_pipeline.beam_tmdb_clean_pipeline import beam_tmdb_clean_pipeline_fn
from python_import_files.normalization.imdb_normalize import normalize_imdb_fn
from python_import_files.normalization.tmdb_normalize import normalize_tmdb_fn
from python_import_files.merge_data import merge_data_fn
from pyspark_files.pyspark_transform import pyspark_transform_fn

defaultArgs = {
    'owner': 'DataAnalyticsCompany',
    'retries':1,
    'retry_delay':timedelta(seconds=15)
}

today = datetime.date(pendulum.today(tz="UTC"))

def merge_data_fn_check():
    cnt_rows_condition = merge_data_fn()
    if cnt_rows_condition:
        # Found more than 10 rows
        return "push_data_mysql"
    else:
        return "email_operator"


def mysql_schema_data_push():
    hook = MySqlHook(mysql_conn_id="mysql_conn", schema="mysql")
    source = hook.get_conn()
    cursor = source.cursor()
    ## Remove DROP TABLE as we will not need it once the project is complete.
    sql_query = f'''
CREATE TABLE IF NOT EXISTS merge_data (
    processed_date DATE,
    source VARCHAR(10),
    movie_id VARCHAR(50),
    title VARCHAR(100),
    media_type VARCHAR(20),
    is_adult VARCHAR(10), 
    release_date DATE,
    languages VARCHAR(20),
    genres TEXT,
    rating FLOAT,
    vote_count INT,
    metascore VARCHAR(20),
    popularity VARCHAR(20),
    runtime VARCHAR(20),
    imdb_movie_id VARCHAR(50),
    normalized_score FLOAT
);
LOAD DATA INFILE '/var/lib/mysql-files/merged_data_clean_{today}_.csv' INTO TABLE merge_data
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
'''
    for query in sql_query.strip().split(";"):
        query = query.strip()
        if query:
            print(query)
            cursor.execute(query+";")
    cursor.execute("SELECT * FROM merge_data;")
    rows = cursor.fetchall()
    for row in rows:
        logging.info(row)
    source.commit()
    cursor.close()
    source.close()



with DAG(dag_id = "MovieRecommendationPipeline",schedule_interval=timedelta(days=1),\
         start_date=pendulum.datetime(2025,5,16,tz="UTC"), catchup=False,tags=["Movie","Project"]) as dag:
    t1 = PythonOperator(
        task_id = "fetch_api_tmdb",
        python_callable=get_tmdb_api
    )

    t2 = PythonOperator(
        task_id = "fetch_api_imdb",
        python_callable=get_imdb_api
    )

    t3 = PythonOperator(
        task_id = "TMDB_clean_beam",
        python_callable=beam_tmdb_clean_pipeline_fn
    )

    t4 = PythonOperator(
        task_id = "IMDB_clean_beam",
        python_callable=beam_imdb_clean_pipeline_fn
    )

    t5 = PythonOperator(
        task_id = "normalize_tmdb_data",
        python_callable=normalize_tmdb_fn
    )

    t6 = PythonOperator(
        task_id = "normalize_imdb_data",
        python_callable=normalize_imdb_fn
    )

    t7 = BranchPythonOperator(
        task_id = "merge_clean_normalize",
        python_callable=merge_data_fn_check
    )

    t8 = EmptyOperator(
        task_id = "email_operator"
    )

    t9 = PythonOperator(
        task_id = "push_data_mysql",
        python_callable=mysql_schema_data_push
    )

    t10 = PythonOperator(
        task_id = "Pyspark_transform",
        python_callable=pyspark_transform_fn
    )

    t11 = BashOperator(
        task_id = "files_format",
        bash_command=f'''
mkdir /opt/airflow/store_files/{today} -p &&
mv /opt/airflow/store_files/rawDataTMDB_{today}.csv /opt/airflow/store_files/{today}/rawDataTMDB_{today}.csv &&
mv /opt/airflow/store_files/rawDataIMDB_{today}.csv /opt/airflow/store_files/{today}/rawDataIMDB_{today}.csv &&
mv /opt/airflow/store_files/clean_TMDB_{today}-00000-of-00001.txt /opt/airflow/store_files/{today}/clean_TMDB_{today}-00000-of-00001.txt &&
mv /opt/airflow/store_files/clean_IMDB_{today}-00000-of-00001.txt /opt/airflow/store_files/{today}/clean_IMDB_{today}-00000-of-00001.txt &&
mv /opt/airflow/store_files/clean_TMDB_{today}_normalized.csv /opt/airflow/store_files/{today}/clean_TMDB_{today}_normalized.csv &&
mv /opt/airflow/store_files/clean_IMDB_{today}_normalized.csv /opt/airflow/store_files/{today}/clean_IMDB_{today}_normalized.csv &&
mv /opt/airflow/store_files/merged_data_clean_{today}_.csv /opt/airflow/store_files/{today}/merged_data_clean_{today}_.csv &&
mv /opt/airflow/store_files/pyspark_output_{today}_.xlsx /opt/airflow/store_files/{today}/pyspark_output_{today}_.xlsx &&
mkdir /opt/airflow/store_files/powerbi_input -p &&
cp /opt/airflow/store_files/{today}/pyspark_output_{today}_.xlsx /opt/airflow/store_files/powerbi_input/pyspark_output_today_.xlsx
'''
    )


    t1 >> t3 >> t5
    t2 >> t4 >> t6
    [t5, t6] >> t7 >> [t8, t9]
    t9 >> t10 >> t11
