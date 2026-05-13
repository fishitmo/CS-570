import streamlit as st
import sys
import os

# Make src/ importable from app/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------
st.set_page_config(
    page_title="CS570 — MovieLens Analytics",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ----------------------------------------------------------------------

with st.sidebar:
    st.title("🎬 MovieLens Analytics")
    st.caption("CS570 · Spring 2026")
    st.divider()
    st.markdown("""
    **Navigation**
    Use the pages above to explore:
    - 📊 Data Overview
    - 🔍 EDA & Charts
    - ⭐ Top Movies
    - 🤖 Recommendations
    - 🧑‍💼 Occupation
    - 📅 Decade Trends
    """)
    st.divider()
    st.caption("Dataset: MovieLens 1M  \n1M ratings · 6K users · 3.9K movies")

# ------------------------------------------------------------------------
st.title("🎬 MovieLens 1M — Big Data Analytics")
st.subheader("CS570 Project · Spring 2026")

st.markdown("""
This dashboard presents an end-to-end analysis of the
[MovieLens 1M dataset](https://grouplens.org/datasets/movielens/1m/),
built with **Apache Spark (PySpark)** and **Streamlit**.
""")

col1, col2, col3 = st.columns(3)
col1.metric("Ratings",  "1,000,209")
col2.metric("Users",    "6,040")
col3.metric("Movies",   "3,952")


# -----------------------------------------------------------------------------

st.divider()
st.subheader("Dataset Files")

st.markdown("""
| File | Columns | Rows |
|------|---------|------|
| `ratings.dat` | UserID, MovieID, Rating, Timestamp | 1,000,209 |
| `users.dat` | UserID, Gender, Age, Occupation, ZipCode | 6,040 |
| `movies.dat` | MovieID, Title, Genres | 3,952 |

**Delimiter:** `::` (double colon)  
**Rating scale:** 1 to 5 stars
""")

# -----------------------------------------------------------------------------

st.divider()
st.subheader("Tech Stack")

c1, c2, c3, c4 = st.columns(4)
c1.info("⚡ **PySpark 3.5**  \nBig data processing")
c2.info("📊 **Streamlit**  \nInteractive dashboard")
c3.info("📈 **Plotly**  \nVisualizations")
c4.info("🔁 **GitHub Actions**  \nCI / CD pipeline")
