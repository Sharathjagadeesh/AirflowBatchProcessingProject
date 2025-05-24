def beam_imdb_clean_pipeline_fn():
    import apache_beam as beam
    import pendulum
    from datetime import datetime
    from ast import literal_eval
    import json

    today = datetime.date(pendulum.today(tz="UTC"))

    input_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\rawDataIMDB_{today}.csv"
    input_file_path_airflow = f"/opt/airflow/store_files/rawDataIMDB_{today}.csv"
    output_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_IMDB_{today}"
    output_file_path_airflow = f"/opt/airflow/store_files/clean_IMDB_{today}"

    options = beam.options.pipeline_options.PipelineOptions(auto_unique_lables=True)


    def format_data(element):
        element = dict(element)

        source = "IMDB"
        movie_id = str(element["id"]).strip()
        title = str(element["primaryTitle"]).strip()
        media_type = str(element["type"]).strip()
        is_adult = bool(element["isAdult"])
        release_date = str(element["releaseDate"]).strip()
        if element["spokenLanguages"]:
            languages = str(element["spokenLanguages"][0]).strip()
        else: languages = "Unkown"
        genres = list(element["genres"])
        if element["averageRating"]:
            rating = float(element["averageRating"])
        else: rating = 0

        if element["numVotes"]:
            vote_count = int(element["numVotes"])
        else: vote_count = 0
        
        if element["metascore"]:
            metascore = int(element["metascore"])
        else: metascore = 0
    
        popularity = None

        if element["runtimeMinutes"]:
            runtime	= int(element["runtimeMinutes"])
        else: runtime = 0
        imdb_movie_id = movie_id
        return source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id


    def filter_results(element):
        source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id = element
        if title and release_date and rating and vote_count and metascore:
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
