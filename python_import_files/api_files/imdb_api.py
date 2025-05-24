def get_imdb_api():
    import requests
    import json
    from datetime import datetime
    import pendulum
    import logging

    today = datetime.date(pendulum.today(tz="utc"))
    logging.basicConfig(level=logging.INFO)

    rapidapi_key_local = r"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\credentials\rapidapi_key.txt"
    output_rapidapi_imdb_file_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\rawDataIMDB_{today}.csv"
    rapidapi_key_airflow = "/opt/airflow/credentials/rapidapi_key.txt"
    output_rapidapi_imdb_file_airflow = f"/opt/airflow/store_files/rawDataIMDB_{today}.csv"
    
    rapidapi_key = ""
    with open(rapidapi_key_airflow, "r") as file:
        data = file.read()
        data = data.strip()
        rapidapi_key = data


    headers = {
        'x-rapidapi-key': f"{rapidapi_key}",
        'x-rapidapi-host': "imdb236.p.rapidapi.com"
    }
    url_imdb = "https://imdb236.p.rapidapi.com/api/imdb/most-popular-movies"
    
    res = requests.get(url=url_imdb,headers=headers)
    
    logging.info(f"status code: {res.status_code}")

    if res.status_code == 200:
        res = res.text
        res = json.loads(res)
        with open(output_rapidapi_imdb_file_airflow, "w") as file:
            for movie in res:
                file.write(str(movie)+"\n")

