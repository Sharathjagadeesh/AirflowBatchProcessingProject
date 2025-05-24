def pyspark_transform_fn():
    
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, BooleanType, DateType
    import pyspark.sql.functions as f 
    import pyspark.pandas as ps
    from ast import literal_eval
    from datetime import datetime
    import math
    from pyspark.sql import Window
    import pendulum

    today = datetime.date(pendulum.today(tz="UTC"))


    spark = SparkSession.builder.master("local[*]").appName("PysparkTransformAirflowProject").getOrCreate()

    merged_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\merged_data_clean_{today}_.csv"
    output_file_path_local = fr"D:\Data Engineering\Projects\BatchProcessingProject\Airflow\docker-airflow-master\store_files\pyspark_output_{today}_.xlsx"

    merged_file_path_airflow = f"/opt/airflow/store_files/merged_data_clean_{today}_.csv"
    output_file_path_airflow = f"/opt/airflow/store_files/pyspark_output_{today}_.xlsx"

    '''
    Rules:
    columns not cannot be null: processed_date,source,movie_id,title,release_date,rating,vote_count,normalized_score
    There is only one language so no need of is_multilanguage
    The runtime is only there for IMDB not for TMDB so no use of content_duration
    The composite score is not useful as I have already done this and the result is the normalized score
    '''

    schema = StructType([
        StructField("processed_date", DateType(),False),
        StructField("source", StringType(),False),
        StructField("movie_id", StringType(),False),
        StructField("title", StringType(),False),
        StructField("media_type", StringType(),True),
        StructField("is_adult", BooleanType(),True),
        StructField("release_date", DateType(),False),
        StructField("language", StringType(),True),
        StructField("genres", StringType(),True),
        StructField("rating", DoubleType(),False),
        StructField("vote_count", IntegerType(),False),
        StructField("metascore", IntegerType(),True),
        StructField("popularity", DoubleType(),True),
        StructField("runtime", IntegerType(),True),
        StructField("imdb_movie_id", StringType(),True),
        StructField("normalized_score", DoubleType(),False)
        ])

    df = spark.read.option("header", "true").schema(schema).csv(merged_file_path_airflow)


    # df.printSchema()


    pandas_df = ps.DataFrame(df)

    def transform_genre(genres):
        genres = literal_eval(genres)
        return genres

    def extract_year(release_date):
        year = release_date.year
        return year

    def decade_extract(release_date):
        year = release_date.year
        res = year/10
        res = math.floor(res)*10
        return res
        
        
    def genre_count(element):
        return len(element)

    def block_buster_check_imdb(vote_count):
        if vote_count >= 10000:
            return True
            
        else:
            return False
        
    def block_buster_check_tmdb(vote_count):
        if vote_count >= 5000:
            return True
            
        else:
            return False
        

    pandas_df["genres"] = pandas_df["genres"].transform(transform_genre)
    pandas_df["release_year"] = pandas_df["release_date"].transform(extract_year)
    pandas_df["decade"] = pandas_df["release_date"].transform(decade_extract)
    pandas_df["is_classic"] = pandas_df["release_date"].transform(lambda x : True if x.year<2000 else False)
    pandas_df["genres_count"] = pandas_df["genres"].apply(genre_count)


    df = pandas_df.to_spark()

    df = df.fillna(0,["runtime", "metascore","popularity"])

    df_imdb = df.filter(df.source == "IMDB")

    pandas_df_imdb = ps.DataFrame(df_imdb)
    pandas_df_imdb["is_blockbuster"] = pandas_df_imdb["vote_count"].transform(block_buster_check_imdb)
    df_imdb = pandas_df_imdb.to_spark()


    df_tmdb = df.filter(df["source"] == "TMDB")

    pandas_df_tmdb = ps.DataFrame(df_tmdb)
    pandas_df_tmdb["is_blockbuster"] = pandas_df_tmdb["vote_count"].transform(block_buster_check_tmdb)
    df_tmdb = pandas_df_tmdb.to_spark()


    df = df_imdb.union(df_tmdb)

    # df.show(vertical=True, n=20)
    '''
    Ranking & Aggregation
    Top N Movies:

    Per genre, language, release_year using ROW_NUMBER() or RANK()

    Aggregations:

    Count of movies per genre

    Average rating or score per decade

    Vote count distribution

    Genre vs average popularity matrix

    Window Functions:

    Running average rating over years

    Dense rank of movies by popularity per year
    '''

    windowSpec = Window.partitionBy("language").orderBy(f.desc("rating"))
    df_language = df.withColumn("rank_language", f.dense_rank().over(windowSpec))
    df_language = df_language.filter(df_language["rank_language"].cast(IntegerType()) <= 2)
    # df.show(vertical=True)


    windowSpec = Window.partitionBy("release_year").orderBy(f.desc("rating"))
    df_release_year = df.withColumn("rank_release_year", f.dense_rank().over(windowSpec))
    df_release_year = df_release_year.filter(df_release_year["rank_release_year"].cast(IntegerType()) <= 2)
    # df_release_year.show(vertical=True)
    # df.printSchema()

    df_genre_rank = df.withColumn("genre", f.explode(df["genres"]))
    windowSpec = Window.partitionBy("genre").orderBy(f.desc("rating"))
    df_genre_rank = df_genre_rank.withColumn("dense_rank_genre", f.dense_rank().over(windowSpec))
    df_genre_rank = df_genre_rank.filter(df_genre_rank["dense_rank_genre"].cast(IntegerType())<=2)
    # df_genre_rank.show(vertical=True)

    df_count_movies_per_genre = df.withColumn("genre", f.explode(df["genres"]))
    df_count_movies_per_genre = df_count_movies_per_genre.groupBy("genre").agg(f.count("*").alias("genre_cnt")).orderBy(f.desc("genre_cnt"))

    # df_count_movies_per_genre.show()


    df_avg_decade_rating = df.groupBy("decade").agg(f.round(f.avg("rating"),2).alias("avg_rating")).orderBy(f.desc("avg_rating"))


    # df_avg_decade_rating.show(vertical=True)

    max_vote_count = df.agg(f.max(df["vote_count"]).alias("max_cnt")).collect()[0].max_cnt

    # Dropping vote count variation as it can be done using bins in powerbi

    # Calcualting for normalized score instead of popularity as popularity can be null
    df_genre_normalized_score = df.withColumn("genre", f.explode(df["genres"]))
    df_genre_normalized_score = df_genre_normalized_score.groupBy("genre").agg(f.round(f.avg("normalized_score"),2).alias("avg_normalized_score")).orderBy(f.desc("avg_normalized_score"))

    # df_genre_normalized_score.show(vertical=True)

    df_ordered = df.orderBy("release_year")
    df_ordered = df_ordered.withColumn("row_count", f.lit(1))
    df_ordered_release_year = df_ordered.groupBy("release_year").agg(f.sum(df_ordered["rating"]).alias("sum_rating"))
    df_ordered_row_count = df_ordered.groupBy("release_year").agg(f.sum("row_count").alias("sum_row_count"))

    df_ordered = df_ordered_release_year.join(df_ordered_row_count, df_ordered_row_count.release_year == df_ordered_release_year.release_year, how="inner").select(df_ordered_release_year.release_year, df_ordered_release_year.sum_rating, df_ordered_row_count.sum_row_count)
    # print(f"Row count of grouped: {df_ordered_row_count.count()}, Row count of joined: {df_ordered.count()}")
    df_ordered = df_ordered.withColumn("static", f.lit(1))
    windowSpec = Window.partitionBy("static").orderBy("release_year")
    df_ordered = df_ordered.withColumn("running_rating", f.sum("sum_rating").over(windowSpec))
    df_ordered = df_ordered.withColumn("running_row_count", f.sum("sum_row_count").over(windowSpec))
    df_ordered = df_ordered.withColumn("running_avg",  f.round( (f.col("running_rating") / f.col("running_row_count")) ,2 ) )

    # Dense rank for popularity
    windowSpec = Window.partitionBy("release_year").orderBy(f.desc("popularity"))
    df_dense_rank = df.withColumn("dense_rank", f.dense_rank().over(windowSpec) )

    df = df.withColumn("genres", f.col("genres").cast(StringType()))

    df.show(vertical=True)

    pandas_df_output = ps.DataFrame(df)
    pandas_df_output.to_excel(output_file_path_airflow)
    # pandas_df_output.to_csv(output_file_path_airflow, num_files=1)



    spark.stop()