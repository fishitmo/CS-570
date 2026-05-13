import streamlit as st
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
import plotly.express as px

from src.spark_session import get_spark
from src.data_loader import load_ratings, load_users, load_movies, join_all
from src.feature_engineering import build_features
from src.eda import (
    count_unique_genres,
    avg_rating_age_group,
    top_movie_by_ratings,
    rating_distribution_chart,
    top_genres_chart,
    avg_rating_by_gender_chart,
    correlation_matrix,
    feature_target_summary,
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

# ── Cache helpers ─────────────────────────────────────────────────────────────

@st.cache_resource
def get_spark_cached():
    return get_spark()

@st.cache_resource
def get_joined():
    spark   = get_spark_cached()
    ratings = load_ratings(spark)
    users   = load_users(spark)
    movies  = load_movies(spark)
    return join_all(ratings, users, movies)

# D1 cached computations
@st.cache_data
def cached_unique_genres():
    return count_unique_genres(get_joined())

@st.cache_data
def cached_avg_rating():
    return avg_rating_age_group(get_joined(), age_code=25)

@st.cache_data
def cached_top_movie():
    return top_movie_by_ratings(get_joined())

@st.cache_data
def cached_rating_chart():
    return rating_distribution_chart(get_joined())

@st.cache_data
def cached_genres_chart():
    return top_genres_chart(get_joined())

@st.cache_data
def cached_gender_chart():
    return avg_rating_by_gender_chart(get_joined())

# D2 cached computations — build features once, then work in pandas
@st.cache_data
def get_featured_sample():
    """5 000-row pandas sample of the fully-engineered DataFrame."""
    df = build_features(get_joined())
    return df.sample(fraction=0.005, seed=42).toPandas()

@st.cache_data
def cached_corr_matrix():
    pdf = get_featured_sample()
    return pdf[NUMERIC_FEATURES].corr().round(3)

@st.cache_data
def cached_feature_summary():
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

st.title("🔍 Exploratory Data Analysis")
st.caption("Full 1M-row analysis via PySpark · D1 questions + D2 feature correlations")

with st.spinner("Running Spark jobs…"):
    n_genres         = cached_unique_genres()
    avg_rating       = cached_avg_rating()
    top_title, top_n = cached_top_movie()

st.success("Analysis complete.")

# ═══════════════════════════════════════════════════════════════════════════════
# D1 — Baseline EDA
# ═══════════════════════════════════════════════════════════════════════════════

st.divider()
st.subheader("D1 · Baseline EDA")

# Metric cards
col1, col2, col3 = st.columns(3)
col1.metric(
    label="A · Unique Genres",
    value=n_genres,
    help="Individual genres extracted from the pipe-separated Genres column",
)
col2.metric(
    label="B · Avg Rating (Age 25–34)",
    value=avg_rating,
    help="Average rating given by users in the 25–34 bracket (Age code = 25)",
)
col3.metric(
    label="C · Most Rated Movie",
    value=f"{top_n:,} ratings",
    delta=top_title,
    delta_color="off",
    help="Movie with the highest number of ratings",
)

st.divider()
st.subheader("📊 Rating Distribution")
st.plotly_chart(cached_rating_chart(), use_container_width=True)

st.subheader("🎭 Genre Popularity")
st.plotly_chart(cached_genres_chart(), use_container_width=True)

st.subheader("👥 Average Rating by Gender")
st.plotly_chart(cached_gender_chart(), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# D2 — Feature Correlation Analysis
# ═══════════════════════════════════════════════════════════════════════════════

st.divider()
st.subheader("D2 · Feature Correlation Analysis")
st.markdown(
    "Using the **17 engineered features** built in D2. "
    "Analysis runs on a 5 000-row sample — first load takes ~60 s, then cached."
)

with st.spinner("Building engineered features…"):
    corr_matrix  = cached_corr_matrix()
    feat_summary = cached_feature_summary()

# 1. Correlation heatmap
st.markdown("#### Pearson Correlation Matrix")
st.markdown(
    "Shows how every numeric feature relates to every other. "
    "**Red** = positive correlation, **Blue** = negative."
)

fig_heat = px.imshow(
    corr_matrix,
    text_auto=True,
    color_continuous_scale="RdBu_r",
    zmin=-1, zmax=1,
    title="Pearson Correlation Matrix — engineered numeric features",
    aspect="auto",
)
st.plotly_chart(fig_heat, use_container_width=True)

st.divider()

# 2. Feature vs target bar chart
st.markdown("#### Feature Importance vs Target (`high_rating`)")
st.markdown(
    "Pearson correlation of each feature with the target label `high_rating` (Rating ≥ 4). "
    "Features closest to **±1** are the strongest predictors."
)

fig_bar = px.bar(
    feat_summary,
    x="Correlation",
    y="Feature",
    orientation="h",
    color="Correlation",
    color_continuous_scale="RdBu_r",
    range_color=[-1, 1],
    title="Correlation with high_rating — sorted by absolute value",
    labels={"Correlation": "Pearson r"},
)
fig_bar.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_bar, use_container_width=True)

# 3. Ranked table
st.markdown("#### Ranked Table")
st.dataframe(feat_summary, use_container_width=True, hide_index=True)

st.markdown("""
**Reading the table:**
- `user_movie_interaction` near the top → the product of user and movie average ratings
  is the strongest single predictor of whether a rating will be ≥ 4.
- `num_genres` near zero → how many genres a movie has barely affects ratings.
""")
