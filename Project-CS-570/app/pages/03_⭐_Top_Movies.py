import streamlit as st
# st.title("⭐ Top Movies")
# st.info("Coming soon — top movies by rating count and score.")

import streamlit as st
import plotly.express as px
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.spark_session import get_spark
from src.data_loader import load_ratings, load_users, load_movies, join_all
from src.eda import top_n_movies_by_count, top_n_movies_by_avg


# ------- Cache SparkSession and joined DataFrame


@st.cache_resource
def get_spark_cached():
    return get_spark()

@st.cache_resource
def get_joined():
    spark  = get_spark_cached()
    ratings = load_ratings(spark)
    users   = load_users(spark)
    movies  = load_movies(spark)
    return join_all(ratings, users, movies)

# ------- Cache the top 50 results ONCE

@st.cache_data
def cached_top_by_count():
    return top_n_movies_by_count(get_joined(), n=50)

@st.cache_data
def cached_top_by_avg():
    return top_n_movies_by_avg(get_joined(), n=50, min_ratings=100)


# ------ Page title and load 

st.title("⭐ Top Movies")
st.caption("Ranked by rating count and average score — computed on 1M ratings")

with st.spinner("Loading from Spark..."):
    df_by_count = cached_top_by_count()
    df_by_avg   = cached_top_by_avg()

st.success("Loaded.")


# Interactive slider in the sidebar

with st.sidebar:
    st.header("Filters")
    top_n = st.slider(
        label="Number of movies to show",
        min_value=5,
        max_value=50,
        value=10,
        step=5,
    )
    min_r = st.number_input(
        label="Min ratings (for avg chart)",
        min_value=10,
        max_value=500,
        value=100,
        step=10,
    )



# Two tabs : by count and by average rating 

st.divider()
tab1, tab2 = st.tabs(["🏆 Most Rated", "⭐ Highest Rated"])

with tab1:
    st.subheader(f"Top {top_n} Movies by Number of Ratings")

    # Slice the cached Pandas DataFrame — no Spark needed
    sliced = df_by_count.head(top_n).copy()
    sliced.index = range(1, len(sliced) + 1)   # rank starting at 1

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.dataframe(
            sliced[["Title", "rating_count", "avg_rating"]],
            use_container_width=True,
        )

    with col_right:
        fig = px.bar(
            sliced,
            x="rating_count",
            y="Title",
            orientation="h",
            title=f"Top {top_n} — Rating Count",
            labels={"rating_count": "Number of Ratings", "Title": ""},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader(f"Top {top_n} Movies by Average Rating")
    st.caption(f"Only movies with at least {min_r} ratings shown")

    # Filter by the sidebar min_ratings input, then take top_n
    filtered = (
        df_by_avg[df_by_avg["rating_count"] >= min_r]
        .head(top_n)
        .copy()
    )
    filtered.index = range(1, len(filtered) + 1)

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.dataframe(
            filtered[["Title", "avg_rating", "rating_count"]],
            use_container_width=True,
        )

    with col_right:
        fig2 = px.bar(
            filtered,
            x="avg_rating",
            y="Title",
            orientation="h",
            title=f"Top {top_n} — Average Rating",
            labels={"avg_rating": "Average Rating", "Title": ""},
            range_x=[3.5, 5.0],
        )
        fig2.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig2, use_container_width=True)


