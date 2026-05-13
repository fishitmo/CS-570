from pyspark.sql import DataFrame
from pyspark.sql.functions import col

VALID_RATINGS = {1.0 , 2.0 , 3.0, 4.0, 5.0}
VALID_AGE_CODES = {1, 18, 25, 35, 45, 50, 56}

def check_duplicate_ratings(ratings: DataFrame) -> dict:
    total = ratings.count()
    unique = ratings.select("UserID", "MovieID").distinct().count()
    bad_count = total - unique
    return {
        "check": "Duplicate Ratings",
        "passed": bad_count == 0,
        "bad_count": bad_count,
        "detail": f"{total:,} rows | {unique:,} unique pairs | duplicates: {bad_count}",
    }
def check_userid_integrity(ratings: DataFrame, users: DataFrame) -> dict:
    rating_users = ratings.select("UserID").distinct()
    orphans      = rating_users.subtract(users.select("UserID")).count()
    return {
        "check": "UserID Referential Integrity",
        "passed": orphans == 0,
        "bad_count": orphans,
        "detail": f"UserIDs in ratings not found in users table: {orphans}",
    }    
    
def check_movieid_integrity(ratings: DataFrame, movies: DataFrame) -> dict:
    rating_movies = ratings.select("MovieID").distinct()
    orphans      = rating_movies.subtract(movies.select("MovieID")).count()
    return {
        "check": "MovieID Referential Integrity",
        "passed": orphans == 0,
        "bad_count": orphans,
        "detail": f"MovieIDs in ratings not found in movies table: {orphans}",
    }  

def check_rating_values(df: DataFrame) -> dict:
    valid_list = [float(v) for v in VALID_RATINGS]
    bad_count = df.filter(~col("Rating").isin(valid_list)).count()
    return {
        "check": "Rating Value Validity",
        "passed": bad_count == 0,
        "bad_count": bad_count,
        "detail": f"Rows with Rating outside {sorted(VALID_RATINGS)}: {bad_count}",
    } 
def check_age_codes(df: DataFrame) -> dict:
    valid_list = list(VALID_AGE_CODES)
    bad_count = df.filter(~col("Age").isin(valid_list)).count()
    return {
        "check": "Age Code Validity",
        "passed": bad_count == 0,
        "bad_count": bad_count,
        "detail": f"Rows with Age outside {sorted(VALID_AGE_CODES)}: {bad_count}",
    }   
    
def check_occupation_codes(df:DataFrame) -> dict:
    bad_count = df.filter((col("Occupation")<0) | (col("Occupation")>20)).count()
    return {
        "check": "Occupation Code Validity",
        "passed": bad_count == 0,
        "bad_count": bad_count,
        "detail": f"Rows with Occupation outside [0, 20]: {bad_count}"
    }   
    
def check_nulls(df: DataFrame) -> dict:
    null_counts = {
        col_name: df.filter(col(col_name).isNull()).count()
        for col_name in df.columns
    }
    total_nulls    = sum(null_counts.values())
    cols_with_nulls = {k: v for k, v in null_counts.items() if v>0}
    
    return {
        "check": "Null Audit",
        "passed": total_nulls == 0,
        "bad_count": total_nulls,
        "detail": f"Total nulls: {total_nulls}" + (
            f" - column: {cols_with_nulls}" if cols_with_nulls else " - all columns clean"
        ),
    }

def run_all_checks(ratings: DataFrame, users: DataFrame, movies: DataFrame,
                   joined: DataFrame) -> list:
    return [
        check_duplicate_ratings(ratings),
        check_userid_integrity(ratings, users),
        check_movieid_integrity(ratings, movies),
        check_rating_values(joined),
        check_age_codes(joined),
        check_occupation_codes(joined),
        check_nulls(joined),
    ]