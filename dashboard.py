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
def plot_daily_revenue(daily_df):
    """
    Return table with daily revenue containing date, revenue.
    """
    fig, ax = plt.subplots(figsize=(10,5))
    ax.plot(daily_df["date"], daily_df["revenue"], marker="o")
    ax.set_title("Daily Revenue")
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue (USD)")
    plt.xticks(rotation=45)
    ax.grid(True)
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
            st.table(top5)
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
        best_ids = [int(x) for x in best_ids]
        st.code(json.dumps(best_ids), language="json")

        st.subheader("Daily Revenue Chart")
        daily = analysis.get("daily_revenue", pd.DataFrame())
        if not daily.empty:
            fig = plot_daily_revenue(daily)
            st.pyplot(fig)
        else:
            st.write("No data")
