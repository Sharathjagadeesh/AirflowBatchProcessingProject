def beam_tmdb_clean_pipeline_fn():
    import apache_beam as beam
    import pendulum
    from datetime import datetime
    from ast import literal_eval
    import json
    import requests
    import logging

    today = datetime.date(pendulum.today(tz="UTC"))

    input_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\rawDataTMDB_{today}.csv"
    input_file_path_airflow = f"/opt/airflow/store_files/rawDataTMDB_{today}.csv"
    output_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_TMDB_{today}"
    output_file_path_airflow = f"/opt/airflow/store_files/clean_TMDB_{today}"

    options = beam.options.pipeline_options.PipelineOptions(auto_unique_lables=True)


    def format_data(element):
        element = dict(element)

        source = "TMDB"
        movie_id = str(element["id"]).strip()
        title = str(element["title"]).strip()
        media_type = str(element["media_type"]).strip()
        is_adult = bool(element["adult"])
        release_date = str(element["release_date"]).strip()
        languages = str(element["original_language"]).strip()
        genres = list(element["genre_ids"])
        rating = float(element["vote_average"])
        vote_count = int(element["vote_count"])
        metascore = None
        popularity = float(element["popularity"])
        runtime	= None
        imdb_movie_id = None
        
        token = ""
        logging.basicConfig(level=logging.INFO)


        input_token_file_local = r"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\credentials\api_key_tmdb.txt"
        input_token_file_airflow = "/opt/airflow/credentials/api_key_tmdb.txt"

        with open(input_token_file_airflow, "r") as file:
            data = file.read()
            data = data.strip()
            token = data
        
        url = f"https://api.themoviedb.org/3/movie/{str(element['id'])}/external_ids?api_key={token}"

        response = requests.get(url=url)
        status_code = response.status_code
        if status_code == 200:
            res = response.text
            res = json.loads(res)["imdb_id"]
            imdb_movie_id = str(res)

        else: 
            logging.error(f"Error!\nCould not fetch the api. Status code: {status_code}.")

        return source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id

    GENRE_ID_NAME = {
    28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy",
    80: "Crime", 99: "Documentary", 18: "Drama", 10751: "Family",
    14: "Fantasy", 36: "History", 27: "Horror", 10402: "Music",
    9648: "Mystery", 10749: "Romance", 878: "Sci-Fi", 10770: "TV Movie",
    53: "Thriller", 10752: "War", 37: "Western"
    }

    def genre_mapping(element):
        source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id = element
        res = []
        for id in genres:
            id = int(id)
            if id in GENRE_ID_NAME:
                res.append(GENRE_ID_NAME[id])
        genres = res
        return source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id

    def filter_results(element):
        source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id = element
        if title and release_date and rating and vote_count and popularity:
            return source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id
        else:
            return None
        
    header_filter_stage = "source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id"



    with beam.Pipeline(options=options) as pipeline:

        # Read data

        read_data = (
            pipeline
            | "Read raw data" >> beam.io.ReadFromText(input_file_path_airflow)
            | "Strip" >> beam.Map(lambda x : x.strip())
            | "Convert string to literal evaluate" >> beam.Map(lambda x : literal_eval(x))
        ) 
        
        # Common format

        format_data = (
            read_data
            | "Remap to common column format" >> beam.Map(format_data)
            | "Map the genere ids to actual genres" >> beam.Map(genre_mapping)
        )

        # Filter data

        filter_data = (
            format_data
            | "Filter records" >> beam.Map(filter_results)
            | "Filter None records" >> beam.Filter(lambda x : x is not None)
        )

        # Write to an output file

        output_file = (
            filter_data
            | "Write to a text file" >> beam.io.WriteToText(file_path_prefix=output_file_path_airflow, file_name_suffix=".txt", num_shards=1,header=header_filter_stage)
        )
