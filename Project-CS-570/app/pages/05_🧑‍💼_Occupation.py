import streamlit as st
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2]))

from src.spark_session import get_spark
from src.data_loader import load_ratings, load_users, load_movies, join_all
from src.eda import occupation_analysis, occupation_chart

st.set_page_config(page_title="Occupation Analysis", page_icon="🧑‍💼", layout="wide")
st.title("🧑‍💼 Occupation Analysis")

@st.cache_resource
def get_spark_cached():
    return get_spark()

@st.cache_data
def get_joined():
    spark = get_spark_cached()
    r = load_ratings(spark)
    u = load_users(spark)
    m = load_movies(spark)
    return join_all(r, u, m).toPandas()   # small enough after agg

# Note: join_all returns Spark DF — keep as Spark for the analysis functions
@st.cache_data
def get_occupation_data():
    spark = get_spark_cached()
    r = load_ratings(spark)
    u = load_users(spark)
    m = load_movies(spark)
    joined = join_all(r, u, m)
    return occupation_analysis(joined), occupation_chart(joined)

with st.spinner("Running Spark aggregation..."):
    pdf, fig = get_occupation_data()

col1, col2 = st.columns([1, 2])
with col1:
    st.dataframe(pdf, use_container_width=True)
with col2:
    st.plotly_chart(fig, use_container_width=True)
