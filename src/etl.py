"""
Modular ETL for datasets. Covers individual functions for tasks:
- Data loading
- Cleaning column names
- Cleaning price columns
- Checking proper types of columns
- Adding pair prices
- Extracting dates
- Removing duplicates and corrupted rows
- Main function for general orchestration
"""

from datetime import datetime
import pandas as pd
import os

EUR_TO_USD = 1.2


def load_data_from_folder(path: str) -> pd.DataFrame:
    """
    Load data from folder, connect into one table and transform into dataframe.
    """
    files = [
        os.path.join(path, f)
        for f in os.listdir(path)
        if f.lower().endswith(".csv")
    ]

    dfs = []
    for file in files:
        df = pd.read_csv(file)
        df["source_file"] = os.path.basename(file)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace(":", "")
    )
    return df


def clean_prices(series: pd.Series) -> pd.Series:
    """
    Clean price column. Removing currency symbols, converting EUR -> USD, converting to float.
    """
    s = series.astype(str).str.strip()

    # Checking if currency is EUR
    is_eur = s.str.contains("€") | s.str.contains("eur", case=False)

    # Removing extra characters (anything that is not a full stop, comma, number)
    s = s.str.replace(r"[^0-9.,-]", "", regex=True)

    # Switching comma to full stop
    s = s.str.replace(",", ".", regex=False)

    # Conversion into numbers
    s = pd.to_numeric(s, errors="coerce")

    # Conver EUR -> USD
    s[is_eur] = s[is_eur] * EUR_TO_USD

    return s.round(2)


def parse_timestamp(series: pd.Series) -> pd.Series:
    """
    Parse timestamps with mixed formats like:
    '09:19:51 P.M., 12-Dec-2024', '15-December-2024, 08:44:59 P.M.', '04:49:37 P.M.,03/11/25'
    """
    formats = [
        "%I:%M:%S %p, %d-%b-%Y",       # 09:19:51 P.M., 12-Dec-2024
        "%d-%B-%Y, %I:%M:%S %p",       # 15-December-2024, 08:44:59 P.M.
        "%I:%M:%S %p,%d/%m/%y",        # 04:49:37 P.M.,03/11/25
        "%I:%M:%S %p %Y-%m-%d"         # 12:38:25 A.M. 2024-06-20
    ]

    def try_parse(x):
        x = str(x).replace("A.M.", "AM").replace("P.M.", "PM").strip()
        for fmt in formats:
            try:
                return datetime.strptime(x, fmt)
            except:
                continue
        return pd.NaT

    return series.apply(try_parse)


def ensure_types(df: pd. DataFrame) -> pd.DataFrame:
    """
    Ensuring all types are correct.
    """

    # quantity = int
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

    # unit_price = float & currency conversion
    if "unit_price" in df.columns:
        df["unit_price"] = clean_prices(df["unit_price"])

    # timestamp = datetime
    if "timestamp" in df.columns:
        df["timestamp"] = parse_timestamp(df["timestamp"])

    return df


def add_paid_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add column paid_price = quantity * unit_price.
    """
    df["paid_price"] = (df["quantity"] * df["unit_price"]).round(2)
    return df


def extract_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add columns: date, year, month, day.
    """
    df["date"] = df["timestamp"].dt.date
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    return df


def remove_dupes_and_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates and rows without basic data.
    """

    df = df.drop_duplicates()
    df = df[df["timestamp"].notna()]

    required = ["quantity", "unit_price", "timestamp"]
    for col in required:
        if col in df.columns:
            df = df[df[col].notna()]

    return df


def etl(path: str) -> pd.DataFrame:
    """
    Main ETL function.
    """

    df = load_data_from_folder(path)
    df = clean_column_names(df)
    df = ensure_types(df)
    df = remove_dupes_and_bad_rows(df)
    df = add_paid_price(df)
    df = extract_dates(df)

    df.reset_index(drop=True, inplace=True)
    return df
