from pyspark.sql import DataFrame
from pyspark.sql import functions as F 
import plotly.express as px 
import pandas as pd


# Anlaysis functions

# Function A: count_unique_genres(df)

def count_unique_genres(df: DataFrame) -> int:
    return (
        df.select(F.explode(F.split(F.col("Genres"),"\\|")).alias("genre"))
        .distinct()
        .count()
    )
    
# Function B - avg_rating_age_group(df, age_code=25)

def avg_rating_age_group(df: DataFrame, age_code: int=25) -> float:
    result = (
        df.filter(F.col("Age") == age_code)
        .agg(F.round(F.avg("Rating"), 2).alias("avg_rating"))
        .collect()[0]["avg_rating"]
    )
    return float(result)

# Function C : top_movies_by_ratings(df: DataFrame) -> tuple:
def top_movie_by_ratings(df: DataFrame) -> tuple:
    row = (
        df.groupBy("MovieID", "Title")
        .agg(F.count("*").alias("rating_count"))
        .orderBy(F.desc("rating_count"))
        .first()
    )
    return row["Title"], int(row["rating_count"])

# Plotly Char Functions 

# Chart 1: Rating Distribution 

def rating_distribution_chart(df: DataFrame):
    counts = (
        df.groupBy("Rating")
          .count()
          .orderBy("Rating")
          .toPandas()                 # Spark → Pandas for Plotly
    )
    fig = px.bar(
        counts,
        x="Rating",
        y="count",
        title="Rating Distribution",
        labels={"count": "Number of Ratings"},
    )
    return fig

# Chapter 2 : Top Genres
def top_genres_chart(df: DataFrame, n: int = 15):
    genre_counts = (
        df.select(F.explode(F.split("Genres", "\\|")).alias("genre"))
          .groupBy("genre")
          .count()
          .orderBy(F.desc("count"))
          .limit(n)
          .toPandas()
    )
    fig = px.bar(
        genre_counts.sort_values("count"),  # sort ascending for horizontal chart
        x="count",
        y="genre",
        orientation="h",
        title=f"Top {n} Genres by Rating Count",
    )
    return fig

# Chapter 3 : Avg Rating by Gender
def avg_rating_by_gender_chart(df: DataFrame):
    result = (
        df.groupBy("Gender")
          .agg(F.round(F.avg("Rating"), 3).alias("avg_rating"))
          .toPandas()
    )
    fig = px.bar(
        result,
        x="Gender",
        y="avg_rating",
        title="Average Rating by Gender",
        labels={"avg_rating": "Average Rating"},
    )
    return fig



# ------------------------------- this is an addtional functions top movies by count and by avgerage 

def top_n_movies_by_count(df: DataFrame, n: int = 50) -> pd.DataFrame:
    """Top N movies ranked by number of ratings. Returns Pandas."""
    return (
        df.groupBy("MovieID", "Title")
          .agg(F.count("*").alias("rating_count"),
               F.round(F.avg("Rating"), 2).alias("avg_rating"))
          .orderBy(F.desc("rating_count"))
          .limit(n)
          .toPandas()
    )


def top_n_movies_by_avg(df: DataFrame, n: int = 50, min_ratings: int = 100) -> pd.DataFrame:
    """Top N movies ranked by average rating — only movies with at least min_ratings."""
    return (
        df.groupBy("MovieID", "Title")
          .agg(F.count("*").alias("rating_count"),
               F.round(F.avg("Rating"), 2).alias("avg_rating"))
          .filter(F.col("rating_count") >= min_ratings)
          .orderBy(F.desc("avg_rating"))
          .limit(n)
          .toPandas()
    )
