# tmdb_clean_normalize = r"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_TMDB_2025-05-18_normalized.csv"
# imdb_clean_normalzie = r"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_IMDB_2025-05-18_normalized.csv"


# # imdb_movie_id_title_mapping = {}
# # imdb_movie_id_normlized_score_mapping = {}

# # with open(imdb_clean_normalzie, "r") as file:
# #     header = file.readline()
# #     lines = file.readlines()
# #     for line in lines:
# #         line = line.strip().split(',')
# #         imdb_movie_id = line[1]
# #         imdb_movie_title = line[2]
# #         if imdb_movie_id not in imdb_movie_id_title_mapping:
# #             imdb_movie_id_title_mapping[imdb_movie_id] = imdb_movie_title
        
# #         if imdb_movie_id not in imdb_movie_id_normlized_score_mapping:
# #             imdb_movie_id_normlized_score_mapping[imdb_movie_id] = [line[-1]]
# #         else:
# #             imdb_movie_id_normlized_score_mapping[imdb_movie_id].append(line[-1])


# # with open(tmdb_clean_normalize, "r") as file:
# #     header = file.readline()
# #     lines = file.readlines()
# #     for line in lines:
# #         line = line.strip().split(',')
# #         imdb_movie_id = line[-2]
# #         imdb_movie_title = line[2]
# #         if imdb_movie_id not in imdb_movie_id_title_mapping:
# #             imdb_movie_id_title_mapping[imdb_movie_id] = imdb_movie_title
        
# #         if imdb_movie_id not in imdb_movie_id_normlized_score_mapping:
# #             imdb_movie_id_normlized_score_mapping[imdb_movie_id] = [line[-1]]
# #         else:
# #             imdb_movie_id_normlized_score_mapping[imdb_movie_id].append(line[-1])

# # for key in imdb_movie_id_normlized_score_mapping:
# #     movie_title = imdb_movie_id_title_mapping[key]
# #     if len(imdb_movie_id_normlized_score_mapping[key])>1:
# #         print(f"IMDB ID: {key}, Title: {movie_title} ,Normalized scores: {imdb_movie_id_normlized_score_mapping[key]}")

# from ast import literal_eval
# import csv
# from datetime import datetime
# import pendulum

# today = datetime.date(pendulum.today(tz="UTC"))

# clean_imdb_data_file_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_IMDB_{today}_normalized.csv"
# clean_tmdb_data_file_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\clean_TMDB_{today}_normalized.csv"
# output_file_merged_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\merged_data_clean_{today}_.csv"

# with open(clean_imdb_data_file_local, "r") as file_imdb_read:
#     header = file_imdb_read.readline()
#     header_format = header.strip()
#     header_format = "processed_date,"+header_format # Fix to add processed data

#     csv_reader = csv.reader(file_imdb_read)
#     for row in csv_reader:
#         print(row)

int_number = 3044564
increment_set = 5000
start_cnt = 0
key_map = {increment_set:1}
while start_cnt<(int_number-increment_set):
    start_cnt+=increment_set
    key_map[start_cnt] = 1

print(key_map)
