# -------------- Imports and Path fix -----------

import streamlit as st


import sys 
import os


# Each page is it's own scripts -- needs the path fix independently
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


from src.spark_session import get_spark
from src.data_loader import load_ratings, load_users, load_movies

# ---------- Cache the SparkSession  -------------------------

@st.cache_resource
def get_spark_cached():
    return get_spark()

# --------- Cache each DataFrame as Pandas -------

@st.cache_data
def get_users():
    spark = get_spark_cached()
    return load_users(spark).toPandas()

@st.cache_data
def get_ratings():
    spark = get_spark_cached()
    return load_ratings(spark).toPandas()
@st.cache_data
def get_movies():
    spark = get_spark_cached()
    return load_movies(spark).toPandas()


# -------- Page title and loading spinner ---------

st.title("📊 Data Overview")
# st.info("Coming soon — data loading section.")
st.caption("MovieLens 1M -three source files loaded via Pyspark with explicit schemas")

with st.spinner("Loading data from Spark..."):
    ratings = get_ratings()
    users = get_users()
    movies = get_movies()
    
    
st.success("All three datasets loaded succesfully.")

# -------- Summary metrics row -----
st.divider()
col1, col2, col3 = st.columns(3)
col1.metric("ratings.dat", f"{len(ratings):,} rows", "4 columns")
col2.metric("users.dat",   f"{len(users):,} rows",   "5 columns")
col3.metric("movies.dat",  f"{len(movies):,} rows",  "3 columns")


# ------  Helper function: schema as a display table

def schema_table(pandas_df):
    return pandas_df.dtypes.reset_index().rename(
        columns={"index": "Column", 0: "Type"}
    )


# -------- Show each dataset with tabs ----------

st.divider()
st.subheader("Dataset Details")

tab1, tab2, tab3 = st.tabs(["⭐ Ratings", "👤 Users", "🎬 Movies"])

with tab1:
    st.markdown("**ratings.dat** — UserID · MovieID · Rating (1–5) · Timestamp")
    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.markdown("**Schema**")
        st.dataframe(schema_table(ratings), use_container_width=True)
    with col_b:
        st.markdown("**Sample (5 rows)**")
        st.dataframe(ratings.head(5), use_container_width=True)

with tab2:
    st.markdown("**users.dat** — UserID · Gender · Age · Occupation · ZipCode")
    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.markdown("**Schema**")
        st.dataframe(schema_table(users), use_container_width=True)
    with col_b:
        st.markdown("**Sample (5 rows)**")
        st.dataframe(users.head(5), use_container_width=True)

with tab3:
    st.markdown("**movies.dat** — MovieID · Title · Genres (pipe-separated)")
    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.markdown("**Schema**")
        st.dataframe(schema_table(movies), use_container_width=True)
    with col_b:
        st.markdown("**Sample (5 rows)**")
        st.dataframe(movies.head(5), use_container_width=True)

# -------  Raw row count verification 
st.divider()
st.subheader("Row Count Verification")
st.markdown("""
| File | Expected | Loaded | Match |
|------|----------|--------|-------|
| ratings.dat | 1,000,209 | """ + f"{len(ratings):,}" + """ | """ + ("✅" if len(ratings) == 1000209 else "⚠️") + """ |
| users.dat   | 6,040     | """ + f"{len(users):,}" + """     | """ + ("✅" if len(users) == 6040 else "⚠️") + """ |
| movies.dat  | 3,952     | """ + f"{len(movies):,}" + """     | """ + ("✅" if len(movies) == 3952 else "⚠️") + """ |
""")
