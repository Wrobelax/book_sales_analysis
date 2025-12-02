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
from dateutil import parser
import pandas as pd
import os
import re

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

    # --- Detect EUR vs USD ---
    is_eur = s.str.contains(r"€|eur", case=False)
    # USD is default currency IF contains $ or USD; else treat as USD implicitly
    # No need for is_usd explicitly.

    cleaned = []

    for val in s:
        raw = val.strip()

        # Empty or missing
        if raw == "" or raw.lower() in ["nan", "none", "null"]:
            cleaned.append(None)
            continue

        # --- Normalize cents notation (¢), e.g. "50¢50" -> "50.50"
        # Extract all numbers:
        nums = re.findall(r"\d+", raw)
        if len(nums) == 2 and "¢" in raw:
            # Example: 50¢50 → 50.50 ; 30¢00 → 30.00
            major, minor = nums
            raw = f"{major}.{minor}"

        # Case where it's like "22$75¢" (still 2 numbers)
        elif len(nums) == 2 and re.search(r"[$€]", raw) and "¢" in raw:
            major, minor = nums
            raw = f"{major}.{minor}"

        # If three or more numbers OR weird patterns — fallback to last number group
        # e.g. "USD 50 75" -> try 50.75
        elif len(nums) >= 2:
            major = nums[0]
            minor = nums[1] if len(nums[1]) <= 2 else nums[1][-2:]
            raw = f"{major}.{minor}"

        # --- Remove currency symbols
        raw = re.sub(r"[€$]", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\bUSD\b|\bEUR\b", "", raw, flags=re.IGNORECASE)

        # --- Replace comma decimal separator
        raw = raw.replace(",", ".")

        # --- Remove all non-digit and non-dot except minus
        raw = re.sub(r"[^0-9.\-]", "", raw)

        # --- Fix trailing dots like "69."
        raw = raw.rstrip(".")

        if raw == "" or raw == ".":
            cleaned.append(None)
            continue

        # --- Convert to number
        try:
            num = float(raw)
        except:
            cleaned.append(None)
            continue

        cleaned.append(num)

    s = pd.Series(cleaned, index=series.index)

    # --- EUR to USD conversion ---
    s[is_eur.fillna(False)] = s[is_eur.fillna(False)] * EUR_TO_USD

    return s.round(2)


def parse_timestamp(series: pd.Series) -> pd.Series:
    """
    Parse timestamps with mixed formats like:
    '09:19:51 P.M., 12-Dec-2024', '15-December-2024, 08:44:59 P.M.', '04:49:37 P.M.,03/11/25'
    """

    def clean_text(x: str) -> str:
        if pd.isna(x):
            return ""

        x = str(x)

        # Normalize AM/PM
        x = (
            x.replace("A.M.", "AM")
            .replace("P.M.", "PM")
            .replace("a.m.", "AM")
            .replace("p.m.", "PM")
            .replace("Am", "AM")
            .replace("Pm", "PM")
        )

        # Replace separators ; , → space
        x = x.replace(";", " ").replace(",", " ")

        # Collapse multiple spaces
        x = " ".join(x.split())
        return x

    def try_parse(x):
        x = clean_text(x)

        if not x:
            return pd.NaT

        # Primary smart parser
        try:
            return parser.parse(x, fuzzy=True)
        except:
            pass

        # ISO fallback
        try:
            return datetime.fromisoformat(x.replace(" ", "T"))
        except:
            return pd.NaT

    return series.apply(try_parse)


def ensure_types(df: pd. DataFrame) -> pd.DataFrame:
    """
    Ensuring all types are correct.
    """

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    if "unit_price" in df.columns:
        df["unit_price"] = clean_prices(df["unit_price"])

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
    df["date"] = df["timestamp"].dt.floor("D")
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    return df


def remove_dupes_and_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates and rows without basic data.
    """

    df = df.drop_duplicates()

    # Remove rows with no timestamp after parsing
    if "timestamp" in df.columns:
        df = df[df["timestamp"].notna()]

    # Remove rows missing essential numeric fields
    required = ["quantity", "unit_price"]
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
