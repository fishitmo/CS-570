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
    
# ── Occupation Analysis ──────────────────────────────────────────────────────
OCCUPATION_LABELS = {
    0: "other", 1: "academic/educator", 2: "artist", 3: "clerical/admin",
    4: "college/grad student", 5: "customer service", 6: "doctor/health care",
    7: "executive/managerial", 8: "farmer", 9: "homemaker", 10: "K-12 student",
    11: "lawyer", 12: "programmer", 13: "retired", 14: "sales/marketing",
    15: "scientist", 16: "self-employed", 17: "technician/engineer",
    18: "tradesman/craftsman", 19: "unemployed", 20: "writer",
}

def occupation_analysis(df):
    """Returns Pandas DF: occupation label, user count, total ratings, avg rating."""
    from pyspark.sql import functions as F
    mapping_expr = F.create_map([F.lit(x) for kv in OCCUPATION_LABELS.items() for x in kv])
    result = (
        df.withColumn("occ_label", mapping_expr[F.col("Occupation")])
          .groupBy("occ_label")
          .agg(
              F.countDistinct("UserID").alias("users"),
              F.count("Rating").alias("total_ratings"),
              F.round(F.avg("Rating"), 3).alias("avg_rating"),
          )
          .orderBy(F.desc("total_ratings"))
    )
    return result.toPandas()


def occupation_chart(df):
    """Horizontal bar chart: avg rating by occupation."""
    import plotly.express as px
    pdf = occupation_analysis(df)
    fig = px.bar(
        pdf, x="avg_rating", y="occ_label", orientation="h",
        color="avg_rating", color_continuous_scale="RdYlGn",
        hover_data=["users", "total_ratings"],
        title="Average Rating by Occupation",
        labels={"occ_label": "Occupation", "avg_rating": "Avg Rating"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    return fig


# ── Movie Release Year / Decade ───────────────────────────────────────────────
def decade_rating_trends(df):
    """Returns Pandas DF: decade, num_movies, num_ratings, avg_rating."""
    from pyspark.sql import functions as F
    result = (
        df.withColumn("year", F.regexp_extract("Title", r"\((\d{4})\)\s*$", 1).cast("int"))
          .filter(F.col("year").isNotNull() & (F.col("year") > 0))
          .withColumn("decade", (F.col("year") / 10).cast("int") * 10)
          .groupBy("decade")
          .agg(
              F.countDistinct("MovieID").alias("num_movies"),
              F.count("Rating").alias("num_ratings"),
              F.round(F.avg("Rating"), 3).alias("avg_rating"),
          )
          .orderBy("decade")
    )
    return result.toPandas()


def decade_chart(df):
    import plotly.graph_objects as go
    pdf = decade_rating_trends(df)
    fig = go.Figure()
    fig.add_bar(x=pdf["decade"].astype(str), y=pdf["num_ratings"], name="# Ratings", yaxis="y1")
    fig.add_scatter(x=pdf["decade"].astype(str), y=pdf["avg_rating"], name="Avg Rating",
                    yaxis="y2", mode="lines+markers", line=dict(color="orange"))
    fig.update_layout(
        title="Movie Decade: Volume vs Quality",
        yaxis=dict(title="# Ratings"),
        yaxis2=dict(title="Avg Rating", overlaying="y", side="right", range=[3, 4.5]),
    )
    return fig


# ── Genre × Age Group ─────────────────────────────────────────────────────────
AGE_LABELS = {1: "<18", 18: "18-24", 25: "25-34", 35: "35-44",
              45: "45-49", 50: "50-55", 56: "56+"}

def genre_age_heatmap(df):
    """Returns Plotly heatmap of avg rating per (age_group, genre)."""
    from pyspark.sql import functions as F
    import plotly.express as px
    mapping_expr = F.create_map([F.lit(x) for kv in AGE_LABELS.items() for x in kv])
    result = (
        df.withColumn("age_label", mapping_expr[F.col("Age")])
          .withColumn("genre", F.explode(F.split("Genres", r"\|")))
          .groupBy("age_label", "genre")
          .agg(
              F.count("Rating").alias("n"),
              F.round(F.avg("Rating"), 2).alias("avg_rating"),
          )
          .filter(F.col("n") >= 200)
    )
    pdf = result.toPandas().pivot(index="age_label", columns="genre", values="avg_rating")
    fig = px.imshow(pdf, text_auto=True, color_continuous_scale="RdYlGn",
                    title="Avg Rating: Age Group × Genre", aspect="auto")
    return fig


# ── D2: Feature Correlation Analysis ─────────────────────────────────────────

def correlation_matrix(df, feature_cols: list) ->"plotly figure":
    """ Pearson correlation heatmap between numeric features and high_rating."""
    import numpy as np
    import plotly.express as px 
    
    pdf = df.select(feature_cols).toPandas()
    corr = pdf.corr()
    
    fig = px.imshow(
        corr,
        text_auto=True,
        color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1,
        title="Pearson Correlation Matrix",
        aspect="auto",
    )
    return fig

def feature_target_summary(df, feature_cols: list) -> pd.DataFrame:
    """Raanked table of each feature's Person correlation with high_rating."""
    cols_to_use = feature_cols + ["high_rating"]
    pdf = df.select(cols_to_use).toPandas()
    
    correlations = pdf.corr()["high_rating"].drop("high_rating")
    
    summary = pd.DataFrame({
        "feature":     correlations.index,
        "correlation": correlations.values.round(4),
        "abs_corr":    correlations.abs().values.round(4),
    }).sort_values("abs_corr", ascending=False).reset_index(drop=True)

    return summary
