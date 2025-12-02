"""
Script for plotting main dashboard.
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
import json
from pipeline import process_dataset


#--------------------
# Daily revenue chart
#--------------------
def plot_revenue_chart(daily):
    """
    Plot daily revenue chart using scatter and rolling trend.
    """
    if daily.empty:
        return None

    daily = daily.sort_values("date")
    x = daily["date"]
    y = daily["revenue"]
    trend = y.rolling(30, min_periods=1).mean()

    # --- PLOT ---
    fig, ax = plt.subplots(figsize=(14, 6))

    ax.scatter(x, y, s=10, alpha=0.5, color="blue", label="Daily Revenue")
    ax.plot(x, trend, color="steelblue", linewidth=2, label="Trend (30-day avg)")

    ax.set_title("Daily Revenue (summed orders) + Trend")
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue (USD)")

    ax.grid(True)
    ax.legend()
    plt.tight_layout()

    return fig


#----------------------------
# Loading and data processing
#----------------------------
BASE_PATH = "data"
DATASETS = ["DATA1", "DATA2", "DATA3"]

all_results = {}

for dataset in DATASETS:
    folder_path = os.path.join(BASE_PATH, dataset)
    if os.path.exists(folder_path):
        st.info(f"Processing {dataset}")
        all_results[dataset] = process_dataset(folder_path)
    else:
        st.warning(f"Folder {folder_path} does not exist.")
        all_results[dataset] = None


# ----------------------------------
# Streamlit UI
# ----------------------------------
st.title("Book Sales Dashboard")

tabs = st.tabs(DATASETS)

for tab, dataset in zip(tabs, DATASETS):
    with tab:
        st.header(f"{dataset} Metrics")

        if not all_results[dataset]:
            st.warning("No data available")
            continue

        analysis = all_results[dataset].get("analysis", {})

        st.subheader("Top 5 Days by Revenue")
        top5 = analysis.get("top5_days", pd.DataFrame())
        if not top5.empty:
            st.dataframe(top5, hide_index=True, width="stretch")
        else:
            st.write("No data")

        st.subheader("Number of Unique Users")
        st.write(analysis.get("unique_users", 0))

        st.subheader("Number of Unique Author Sets")
        st.write(analysis.get("unique_author_sets", 0))

        st.subheader("Most Popular Author / Author Set")
        st.write(analysis.get("most_popular_author_set", []))

        st.subheader("Best Buyer (IDs)")
        best_ids = analysis.get("best_buyer_aliases", [])
        st.code(json.dumps(best_ids), language="json")

        st.subheader("Daily Revenue Chart")
        daily = analysis.get("daily_revenue", pd.DataFrame())
        if not daily.empty:
            fig = plot_revenue_chart(daily)
            st.pyplot(fig)
        else:
            st.write("No data")
