"""
app/pages/08_⚙️_Feature_Engineering.py
D2 — 17 engineered features, correlation heatmap, and feature-target ranking.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import streamlit as st
import pandas as pd
import plotly.express as px

from src.spark_session import get_spark
from src.data_loader import load_ratings, load_users, load_movies, join_all
from src.feature_engineering import build_features

st.set_page_config(
    page_title="Feature Engineering — CS570",
    page_icon="⚙️",
    layout="wide",
)

# ── Constants ─────────────────────────────────────────────────────────────────

NUMERIC_FEATURES = [
    "user_movie_interaction",
    "movie_avg_rating",
    "user_avg_rating",
    "rating_deviation",
    "movie_popularity",
    "log_movie_popularity",
    "movie_age",
    "num_genres",
    "high_rating",
]

FEATURE_CATALOG = [
    ("high_rating",            "Target",  "1 if Rating ≥ 4, else 0"),
    ("movie_avg_rating",       "Movie",   "Average rating of this movie across all users"),
    ("movie_popularity",       "Movie",   "Number of ratings this movie has received"),
    ("log_movie_popularity",   "Movie",   "log(movie_popularity + 1) — reduces right-skew"),
    ("release_year",           "Movie",   "Year extracted from title string, e.g. (1995)"),
    ("movie_age",              "Movie",   "2000 − release_year"),
    ("user_avg_rating",        "User",    "Average rating given by this user"),
    ("user_rating_count",      "User",    "Total number of movies this user has rated"),
    ("rating_deviation",       "User",    "user_avg_rating − global_average_rating"),
    ("user_movie_interaction", "Derived", "user_avg_rating × movie_avg_rating"),
    ("primary_genre",          "Derived", "First genre in the pipe-separated Genres string"),
    ("num_genres",             "Derived", "Number of genres tagged to this movie"),
    ("is_film_noir",           "Derived", "1 if Genres contains Film-Noir"),
    ("is_war",                 "Derived", "1 if Genres contains War"),
    ("is_horror",              "Derived", "1 if Genres contains Horror"),
    ("is_action",              "Derived", "1 if Genres contains Action"),
    ("movie_era",              "Derived", "Classic / Retro / Modern / Contemporary by release_year"),
]

# ── Spark + cached data ───────────────────────────────────────────────────────

@st.cache_resource
def get_spark_cached():
    return get_spark()

@st.cache_data
def get_featured_sample():
    """Build all 17 features on the full dataset, return a ~5 000-row pandas sample."""
    spark   = get_spark_cached()
    ratings = load_ratings(spark)
    users   = load_users(spark)
    movies  = load_movies(spark)
    joined  = join_all(ratings, users, movies)
    df      = build_features(joined)
    return df.sample(fraction=0.005, seed=42).toPandas()

@st.cache_data
def get_corr_matrix():
    pdf = get_featured_sample()
    return pdf[NUMERIC_FEATURES].corr().round(3)

@st.cache_data
def get_feature_summary():
    pdf  = get_featured_sample()
    corr = pdf[NUMERIC_FEATURES].corr()["high_rating"].drop("high_rating")
    return (
        pd.DataFrame({
            "Feature":     corr.index,
            "Correlation": corr.values.round(4),
            "Abs":         corr.abs().values.round(4),
        })
        .sort_values("Abs", ascending=False)
        .reset_index(drop=True)
    )

# ── Page ──────────────────────────────────────────────────────────────────────

st.title("⚙️ Feature Engineering")
st.caption("D2 · 17 derived features built on top of the MovieLens 1M joined dataset")

with st.spinner("Building features on 1,000,209 rows — first run takes ~60 s, then cached…"):
    sample_pdf   = get_featured_sample()
    corr_matrix  = get_corr_matrix()
    feat_summary = get_feature_summary()

st.success(f"Features ready. Showing sample of {len(sample_pdf):,} rows.")

# ── 1. Feature catalog ────────────────────────────────────────────────────────

st.divider()
st.subheader("1 · Feature Catalog — 17 Features")

catalog_df = pd.DataFrame(FEATURE_CATALOG, columns=["Feature", "Group", "Description"])

tab_all, tab_target, tab_movie, tab_user, tab_derived = st.tabs(
    ["All (17)", "Target (1)", "Movie (5)", "User (3)", "Derived (8)"]
)
with tab_all:
    st.dataframe(catalog_df, use_container_width=True, hide_index=True)
with tab_target:
    st.dataframe(catalog_df[catalog_df["Group"] == "Target"],  use_container_width=True, hide_index=True)
with tab_movie:
    st.dataframe(catalog_df[catalog_df["Group"] == "Movie"],   use_container_width=True, hide_index=True)
with tab_user:
    st.dataframe(catalog_df[catalog_df["Group"] == "User"],    use_container_width=True, hide_index=True)
with tab_derived:
    st.dataframe(catalog_df[catalog_df["Group"] == "Derived"], use_container_width=True, hide_index=True)

# ── 2. Sample rows ────────────────────────────────────────────────────────────

st.divider()
st.subheader("2 · Sample Rows (first 20 of the 5 000-row sample)")
st.dataframe(sample_pdf.head(20), use_container_width=True)

# ── 3. Correlation heatmap ────────────────────────────────────────────────────

st.divider()
st.subheader("3 · Feature Correlation Matrix")
st.markdown(
    "Pearson correlation between every pair of numeric features. "
    "Values close to **+1** or **−1** mean strong linear relationship."
)

fig_heat = px.imshow(
    corr_matrix,
    text_auto=True,
    color_continuous_scale="RdBu_r",
    zmin=-1, zmax=1,
    title="Pearson Correlation Matrix — numeric features",
    aspect="auto",
)
st.plotly_chart(fig_heat, use_container_width=True)

# ── 4. Feature vs target ranking ──────────────────────────────────────────────

st.divider()
st.subheader("4 · Feature Importance vs Target (high_rating)")
st.markdown(
    "Pearson correlation of each feature with `high_rating` (the target label). "
    "Positive = correlated with high ratings; negative = correlated with low ratings."
)

fig_bar = px.bar(
    feat_summary,
    x="Correlation",
    y="Feature",
    orientation="h",
    color="Correlation",
    color_continuous_scale="RdBu_r",
    range_color=[-1, 1],
    title="Correlation with high_rating (sorted by absolute value)",
)
fig_bar.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_bar, use_container_width=True)

st.dataframe(feat_summary, use_container_width=True, hide_index=True)

# ── 5. Movie era distribution ─────────────────────────────────────────────────

st.divider()
st.subheader("5 · Movie Era Distribution")

era_counts = sample_pdf["movie_era"].value_counts().reset_index()
era_counts.columns = ["movie_era", "count"]
era_order = ["Classic", "Retro", "Modern", "Contemporary"]
era_counts["movie_era"] = pd.Categorical(era_counts["movie_era"], categories=era_order, ordered=True)
era_counts = era_counts.sort_values("movie_era")

fig_era = px.bar(
    era_counts,
    x="movie_era", y="count",
    title="Ratings by Movie Era",
    labels={"movie_era": "Era", "count": "# Ratings (sample)"},
    color="movie_era",
)
st.plotly_chart(fig_era, use_container_width=True)

# ── 6. Primary genre distribution ────────────────────────────────────────────

st.divider()
st.subheader("6 · Primary Genre Distribution")

genre_counts = (
    sample_pdf["primary_genre"]
    .value_counts()
    .reset_index()
)
genre_counts.columns = ["primary_genre", "count"]

fig_genre = px.bar(
    genre_counts.sort_values("count"),
    x="count", y="primary_genre",
    orientation="h",
    title="Ratings by Primary Genre (sample)",
    labels={"primary_genre": "Genre", "count": "# Ratings"},
)
st.plotly_chart(fig_genre, use_container_width=True)
