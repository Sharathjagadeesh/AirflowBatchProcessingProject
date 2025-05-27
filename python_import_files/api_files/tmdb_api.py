def get_tmdb_api():
    import requests
    import json
    from datetime import datetime
    import pendulum
    import logging

    today = datetime.date(pendulum.today(tz="utc"))
    logging.basicConfig(level=logging.INFO)

    token_tmdb_api_key_airflow = "/opt/airflow/credentials/token_api_tmdb.txt"
    output_tmdb_file_airflow = f"/opt/airflow/store_files/rawDataTMDB_{today}.csv"
    
    token = ""
    with open(token_tmdb_api_key_airflow, "r") as file:
        data = file.read()
        data = data.strip()
        token = data
    org_url = "https://api.themoviedb.org/3/trending/movie/day"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    number_of_pages = 5

    with open(output_tmdb_file_airflow, "w") as file:
        for i in range(1,number_of_pages+1):
            url = f"{org_url}?page={i}"
            response = requests.get(url, headers=headers)
            logging.info(f"status code: {response.status_code}")
            if response.status_code == 200:
                res = response.text
                res = json.loads(res)["results"]
                for movie in res:

                    file.write(str(movie)+"\n")
