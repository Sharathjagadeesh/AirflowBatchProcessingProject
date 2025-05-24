import requests
import json
from datetime import datetime
import pendulum

today = datetime.date(pendulum.today(tz="utc"))

token_tmdb_api_key_local = r"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\credentials\token_api_tmdb.txt"
output_tmdb_file_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\rawDataTMDB_{today}.csv"
token = ""
with open(token_tmdb_api_key_local, "r") as file:
    data = file.read()
    data = data.strip()
    token = data
org_url = "https://api.themoviedb.org/3/trending/movie/day"

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {token}"
}

number_of_pages = 5

with open(output_tmdb_file_local, "w") as file:
    for i in range(1,number_of_pages+1):
        url = f"{org_url}?page={i}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            res = response.text
            res = json.loads(res)["results"]
            for movie in res:

                file.write(str(movie)+"\n")
        


rapidapi_key_local = r"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\credentials\rapidapi_key.txt"
output_rapidapi_imdb_file_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\rawDataIMDB_{today}.csv"
rapidapi_key = ""
with open(rapidapi_key_local, "r") as file:
    data = file.read()
    data = data.strip()
    rapidapi_key = data


headers = {
    'x-rapidapi-key': f"{rapidapi_key}",
    'x-rapidapi-host': "imdb236.p.rapidapi.com"
}
url_imdb = "https://imdb236.p.rapidapi.com/imdb/lowest-rated-movies"
res = requests.get(url=url_imdb,headers=headers)
if res.status_code == 200:
    res = res.text
    res = json.loads(res)
    with open(output_rapidapi_imdb_file_local, "w") as file:
        for movie in res:
            file.write(str(movie)+"\n")


