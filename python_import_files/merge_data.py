def merge_data_fn():
    import csv
    from datetime import datetime
    import pendulum
    import json
    import csv
    from ast import literal_eval
    import logging
    logging.basicConfig(level=logging.INFO)

    today = datetime.date(pendulum.today(tz="UTC"))


    clean_imdb_data_file_airflow = f"/opt/airflow/store_files/clean_IMDB_{today}_normalized.csv"
    clean_tmdb_data_file_airflow = f"/opt/airflow/store_files/clean_TMDB_{today}_normalized.csv"
    output_file_merged_airflow = f"/opt/airflow/store_files/merged_data_clean_{today}_.csv"

    res = {}

    imdb_normalized_values = []

    with open(clean_imdb_data_file_airflow, "r") as file_imdb_read:
        header = file_imdb_read.readline()
        header_format = header.strip()
        header_format = "processed_date,"+header_format # Fix to add processed data

        csv_reader = csv.reader(file_imdb_read, quoting=csv.QUOTE_MINIMAL)

        for line in csv_reader:

            line = [str(today)] + line # '2025-05-19',IMDB,tt20969586,Thunderbolts*,movie,False,2025-05-02,en,"['Action', 'Adventure', 'Crime']",7.6,89004,68,,127,tt20969586,1.03
            res[line[-2]] = line
            normalized_values = float(line[-1])
            imdb_normalized_values.append(normalized_values)
    
    with open(clean_tmdb_data_file_airflow, "r") as file_tmdb_read:
        header = file_tmdb_read.readline()

        csv_reader = csv.reader(file_tmdb_read, quoting=csv.QUOTE_MINIMAL)

        for line in csv_reader:

            line = [str(today)] + line # '2025-05-19',TMDB,tt20969586,Thunderbolts*,movie,False,2025-05-02,en,"['Action', 'Adventure', 'Crime']",7.6,89004,68,,127,tt20969586,1.03
            if line[-2] not in res:
                res[line[-2]] = line



    if len(imdb_normalized_values) !=0:
        addtional_constant = sum(imdb_normalized_values)/len(imdb_normalized_values)
    else:
        addtional_constant = 0
    
    logging.info(f"multiplication constant: {addtional_constant}")

    for key in res:
        source = res[key][1] # IMDB or TMDB
        normalized_score = float(res[key][-1])
        if source.lower().strip() == "tmdb":
            res[key][-1] = round(normalized_score + addtional_constant,2)
    cnt_row = 0
    with open(output_file_merged_airflow, "w", newline="") as file:
        csv_writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(header_format.split(","))
        for key in res:
            genres_index = 8  # Genres are in the 9th column (index 8)
            # res[key][genres_index] = clean_genres(res[key][genres_index])
            csv_writer.writerow(res[key])
            cnt_row+=1
    
    if cnt_row>10:
        return True
    else:
        return False

