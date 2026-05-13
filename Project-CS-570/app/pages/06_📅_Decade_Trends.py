import streamlit as st
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from src.spark_session import get_spark
from src.data_loader import load_ratings, load_users, load_movies, join_all
from src.eda import decade_rating_trends, decade_chart, genre_age_heatmap

st.set_page_config(page_title="Decade Trends", page_icon="📅", layout="wide")
st.title("📅 Movie Decade Trends & Genre × Age Heatmap")

@st.cache_resource
def get_spark_cached():
    return get_spark()

@st.cache_data
def get_data():
    spark = get_spark_cached()
    joined = join_all(load_ratings(spark), load_users(spark), load_movies(spark))
    return decade_chart(joined), decade_rating_trends(joined), genre_age_heatmap(joined)

with st.spinner("Computing..."):
    dchart, dpdf, heatmap = get_data()

st.subheader("🗓️ Ratings Volume & Quality by Decade")
st.plotly_chart(dchart, use_container_width=True)
st.dataframe(dpdf, use_container_width=True)

st.divider()
st.subheader("🎭 Genre Preference by Age Group (Avg Rating Heatmap)")
st.plotly_chart(heatmap, use_container_width=True)
