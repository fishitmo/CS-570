"""
src/feature_engineering.py
──────────────────────────
D2 — Feature Engineering for MovieLens 1M
Builds 17 derived features on the joined DataFrame.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, count, avg, log, regexp_extract, split, size

def add_target_label(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "high_rating",
        when(col("Rating") >= 4,1).otherwise(0)
    )

def add_movie_features(df:DataFrame) -> DataFrame:
    movie_stats = df.groupBy("MovieID").agg(
        avg("Rating").alias("movie_avg_rating"),
        count("Rating").alias("movie_popularity")
    )
    
    df = df.join(movie_stats, on = "MovieID", how="left")
    
    df = df.withColumn("log_movie_popularity", log(col("movie_popularity") + 1))
    
    df = df.withColumn(
        "release_year",
        regexp_extract(col("Title"), r"\((\d{4})\)", 1).cast("int")
        )
    
    df = df.withColumn("movie_age", 2000-col("release_year"))
    
    return df


def add_user_features(df: DataFrame) -> DataFrame:
    user_stats = df.groupBy("UserID").agg(
        avg("Rating").alias("user_avg_rating"),
        count("Rating").alias("user_rating_count")
    )
    
    df  = df.join(user_stats, on = "UserID", how="left")
    
    global_avg = df.agg(avg("Rating")).collect()[0][0]
    
    df = df.withColumn("rating_deviation", col("user_avg_rating") - global_avg)

    return df


def add_derived_features(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "user_movie_interaction", 
        col("user_avg_rating")*col("movie_avg_rating")
    )
    df = df.withColumn("primary_genre", split(col("Genres"),r"\|")[0])
    df = df.withColumn("num_genres", size(split(col("Genres"), r"\|")))
    
    df = df.withColumn("is_film_noir", when(col("Genres").contains("Film-Noir"), 1).otherwise(0))
    df = df.withColumn("is_war",       when(col("Genres").contains("War"),       1).otherwise(0))
    df = df.withColumn("is_horror",    when(col("Genres").contains("Horror"),    1).otherwise(0))
    df = df.withColumn("is_action",    when(col("Genres").contains("Action"),    1).otherwise(0))
    
    df = df.withColumn("movie_era",
        when(col("release_year") < 1970,  "Classic")
        .when(col("release_year") < 1990, "Retro")
        .when(col("release_year") < 1995, "Modern")
        .otherwise("Contemporary")
    )
    
    return df

def build_features(df: DataFrame) -> DataFrame:
    df = add_target_label(df)
    df = add_movie_features(df)
    df = add_user_features(df)
    df = add_derived_features(df)
    return df
