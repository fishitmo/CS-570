"""
app/pages/07_🧹_Data_Quality.py
D2 — Data Quality Checks (7 checks via src/data_cleaning.py)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import streamlit as st
from src.spark_session import get_spark
from src.data_loader import load_ratings, load_users, load_movies, join_all
from src.data_cleaning import run_all_checks

st.set_page_config(
    page_title="Data Quality — CS570",
    page_icon="🧹",
    layout="wide",
)

# ── Spark + data (cached so re-runs are instant) ──────────────────────────────

@st.cache_resource
def get_spark_cached():
    return get_spark()

@st.cache_data
def get_quality_results():
    spark   = get_spark_cached()
    ratings = load_ratings(spark)
    users   = load_users(spark)
    movies  = load_movies(spark)
    joined  = join_all(ratings, users, movies)
    return run_all_checks(ratings, users, movies, joined)

# ── Page ──────────────────────────────────────────────────────────────────────

st.title("🧹 Data Quality Checks")
st.caption("D2 · 7 validation checks across ratings, users, movies, and the joined dataset")

with st.spinner("Running 7 quality checks on 1,000,209 rows…"):
    results = get_quality_results()

# Summary metrics
total  = len(results)
passed = sum(1 for r in results if r["passed"])
failed = total - passed

c1, c2, c3 = st.columns(3)
c1.metric("Total Checks", total)
c2.metric("Passed ✅",    passed)
c3.metric("Failed ❌",    failed, delta_color="inverse",
          delta=f"-{failed}" if failed else None)

st.divider()

# ── Expandable result cards ───────────────────────────────────────────────────

st.subheader("Check Results")

CHECK_DESCRIPTIONS = {
    "Duplicate Ratings":
        "Each (UserID, MovieID) pair should appear at most once in ratings.dat.",
    "UserID Referential Integrity":
        "Every UserID that appears in ratings must exist in the users table.",
    "MovieID Referential Integrity":
        "Every MovieID that appears in ratings must exist in the movies table.",
    "Rating Value Validity":
        "Ratings must be one of {1.0, 2.0, 3.0, 4.0, 5.0} — no decimals or out-of-range values.",
    "Age Code Validity":
        "Age is stored as a code: {1, 18, 25, 35, 45, 50, 56} per the MovieLens spec.",
    "Occupation Code Validity":
        "Occupation codes must be integers in [0, 20] per the MovieLens spec.",
    "Null Audit":
        "No column in the joined DataFrame should contain NULL values.",
}

for r in results:
    icon   = "✅" if r["passed"] else "❌"
    label  = "PASS" if r["passed"] else "FAIL"
    with st.expander(f"{icon} **{r['check']}** — {label}", expanded=not r["passed"]):
        desc = CHECK_DESCRIPTIONS.get(r["check"], "")
        if desc:
            st.markdown(f"**What we check:** {desc}")
        st.markdown(f"**Result:** `{r['detail']}`")
        if r["bad_count"] > 0:
            st.error(f"Issues found: {r['bad_count']:,}")
        else:
            st.success("No issues found — dataset is clean.")

st.divider()

# ── Why these checks matter ───────────────────────────────────────────────────

st.subheader("Why Data Quality Matters")
st.markdown("""
Before building any ML model or computing any statistic, we need to trust the data.
Here is what each category of check protects against:

| Check | Risk if skipped |
|---|---|
| Duplicates | Double-counting ratings — inflates popularity scores |
| Referential Integrity | Joining on orphan IDs → NULL rows in the joined dataset |
| Rating validity | Garbage values would distort average ratings and the target label |
| Age / Occupation codes | Wrong demographic segments — misleading group analysis |
| Null audit | NULL values crash PySpark aggregations silently |
""")
