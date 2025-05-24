def normalize_imdb_fn():
    from datetime import datetime
    import pendulum
    from ast import literal_eval
    import json
    import csv

    today = datetime.date(pendulum.today(tz="UTC"))

    input_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_IMDB_{today}-00000-of-00001.txt"
    input_file_path_airflow  = f"/opt/airflow/store_files/clean_IMDB_{today}-00000-of-00001.txt"
    output_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_IMDB_{today}_normalized.csv"
    output_file_path_airflow = f"/opt/airflow/store_files/clean_IMDB_{today}_normalized.csv"

    header_format = "source,movie_id,title,media_type,is_adult,release_date,languages,genres,rating,vote_count,metascore,popularity,runtime,imdb_movie_id,normalized_score"
    
    vote_count_array = []

    with open(input_file_path_airflow, "r") as file_read:
        header = file_read.readline()
        for line in file_read.readlines():
            line = line.strip()
            line = literal_eval(line)

            vote_count = line[-5]
            if vote_count:
                vote_count_array.append(int(vote_count))
    
    if vote_count_array:
        max_vote_count = max(vote_count_array)
        min_vote_count = min(vote_count_array)
    
    with open(output_file_path_airflow, "w", newline="") as file_write:
        csv_writer = csv.writer(file_write,quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(header_format.split(","))
       
        with open(input_file_path_airflow, "r") as file_read:
            header = file_read.readline()
            for line in file_read.readlines():
                line = line.strip()
                line = literal_eval(line)
                line = list(line)
                if line[-4] and line[-5] and (max_vote_count - min_vote_count)!=0:
                    vote_count_normalize = (vote_count - min_vote_count) / (max_vote_count - min_vote_count)
                    normalized_score = ( int(line[-4]) * vote_count_normalize )/100
                    normalized_score = round(normalized_score * 100 ,2)
                else:
                    normalized_score = 0
                line.append(normalized_score)
                csv_writer.writerow(line)


# normalize_imdb_fn()