"""
Script for running whole pipeline.
"""

import src.etl
from src.analysis import analyze, find_real_user
import os
import pandas as pd
import yaml


def load_books_yaml(path):
    """
    Loading yaml file into DataFrame.
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    df = pd.json_normalize(data)
    return df


def load_users_csv(path):
    """
    Load csv file into DataFrame.
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    return df


def process_dataset(folder_path):
    """
    Processing single folder with all three files:
    - books.yaml
    - users.csv
    - orders.parquet
    """
    orders_file = os.path.join(folder_path, "orders.parquet")
    users_file = os.path.join(folder_path, "users.csv")
    books_file = os.path.join(folder_path, "books.yaml")
    df_orders_full = pd.DataFrame()

    # Process orders
    if os.path.exists(orders_file):
        df_orders = pd.read_parquet(orders_file)
        df_orders["source_file"] = "orders.parquet"

        df_orders = src.etl.clean_column_names(df_orders)
        df_orders = src.etl.ensure_types(df_orders)
        df_orders = src.etl.remove_dupes_and_bad_rows(df_orders)
        df_orders = src.etl.add_paid_price(df_orders)
        df_orders = src.etl.extract_dates(df_orders)
    else:
        df_orders = pd.DataFrame()

    # Process books
    df_books = load_books_yaml(books_file)
    df_books = src.etl.clean_column_names(df_books)
    df_books.rename(columns={"id": "id_book"}, inplace=True)
    df_books = df_books.drop_duplicates(subset=["id_book"])

    # Process users
    df_users = load_users_csv(users_file)
    df_users.rename(columns={"id": "id_user"}, inplace=True)
    df_users = df_users.drop_duplicates(subset=["id_user"])

    # Merge data
    if not df_orders.empty and not df_users.empty and not df_books.empty:
        df_orders_users = df_orders.merge(df_users, left_on="user_id", right_on="id_user", how="left")
        df_orders_full = df_orders_users.merge(df_books, left_on="book_id", right_on="id_book", how="left")

        book_price_cols = [
            c for c in df_orders_full.columns
            if c not in ["unit_price", "paid_price"] and "price" in c
        ]
        df_orders_full.drop(columns=book_price_cols, inplace=True, errors="ignore")


        results = analyze(df_orders_full)

        if "best_buyer_aliases" in results and isinstance(results["best_buyer_aliases"], list):
            results["best_buyer_aliases"] = sorted(set(int(x) for x in results["best_buyer_aliases"]))


    else:
        results = {}



    return {
        "orders": df_orders,
        "books": df_books,
        "users": df_users,
        "orders_full": df_orders_full,
        "analysis": results
    }


def process_all_datasets(base_path="data"):
    """
    Process all dataset folders.
    """
    datasets = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
    results = {}

    for dataset in datasets:
        folder_path = os.path.join(base_path, dataset)
        print(f"Processing {dataset}")
        results[dataset] = process_dataset(folder_path)
        print(f"Dataset {dataset} complete.")

    return results


def save_results_to_csv(results, output_folder="results"):
    """
    Saves pipeline results to csv file.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for ds, res in results.items():
        # Orders
        if not res["orders"].empty:
            res["orders"].to_csv(os.path.join(output_folder, f"{ds}_orders_cleaned.csv"), index=False)

            analysis = res["analysis"]
            if analysis:
                if "top5_days" in analysis:
                    analysis["top5_days"].to_csv(os.path.join(output_folder, f"{ds}_top5_days.csv"), index=False)
                if "daily_revenue" in analysis:
                    analysis["daily_revenue"].to_csv(os.path.join(output_folder, f"{ds}_daily_revenue.csv"), index=False)

        # Books
        if not res["books"].empty:
            res["books"].to_csv(os.path.join(output_folder, f"{ds}_books_cleaned.csv"), index=False)

        # Users
        if not res["users"].empty:
            res["users"].to_csv(os.path.join(output_folder, f"{ds}_users_cleaned.csv"), index=False)

    print(f"All results saved to {output_folder}")


if __name__ == "__main__":
    results = process_all_datasets("data")
    save_results_to_csv(results, "results")
