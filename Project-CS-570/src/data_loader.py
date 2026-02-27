from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, StringType, FloatType,
)
from src.utils import raw_path

# schemas
RATINGS_SCHEMA = StructType([
    StructField("UserID",    IntegerType(), nullable=False),
    StructField("MovieID",   IntegerType(), nullable=False),
    StructField("Rating",    FloatType(),   nullable=False),
    StructField("Timestamp", LongType(),    nullable=False),
])

USERS_SCHEMA = StructType([
    StructField("UserID",     IntegerType(), nullable=False),
    StructField("Gender",     StringType(),  nullable=False),
    StructField("Age",        IntegerType(), nullable=False),
    StructField("Occupation", IntegerType(), nullable=False),
    StructField("ZipCode",    StringType(),  nullable=True),   # voluntary, may be missing
])

MOVIES_SCHEMA = StructType([
    StructField("MovieID", IntegerType(), nullable=False),
    StructField("Title",   StringType(),  nullable=False),
    StructField("Genres",  StringType(),  nullable=False),
])

# loader functions
def load_ratings(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("sep", "::")       # double-colon delimiter — critical!
        .schema(RATINGS_SCHEMA)    # explicit schema — no inferSchema
        .csv(raw_path("ratings.dat"))
    )
    
def load_users(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("sep", "::")
        .schema(USERS_SCHEMA)
        .csv(raw_path("users.dat"))
    )


def load_movies(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("sep", "::")
        .schema(MOVIES_SCHEMA)
        .csv(raw_path("movies.dat"))
    ) 

# Join_all() function 

# def join_all(ratings: DataFrame, users: DataFrame, movies: DataFrame) -> DataFrame:
#     return (
#         ratings
#         .join(???,  on="???",  how="inner")   # what column links ratings ↔ users?
#         .join(???,  on="???",  how="inner")   # what column links ratings ↔ movies?
#     )


def join_all(ratings: DataFrame, users: DataFrame, movies: DataFrame):
    return (
        ratings
        .join(users,  on="UserID",  how="inner")
        .join(movies, on="MovieID", how="inner")
    )

