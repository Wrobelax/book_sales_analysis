"""
Script used for analysis of data.
"""

from typing import Union
import pandas as pd


def compute_daily_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return table with daily revenue: date, revenue.
    """
    return (
        df.groupby("date", as_index=False)["paid_price"]
        .sum()
        .rename(columns={"paid_price": "revenue"})
    )


def get_top5_days(df_daily: pd. DataFrame) -> pd.DataFrame:
    """
    Returns top 5 days per revenue. Date format: YYY-MM-DD (str).
    """
    top5 = df_daily.sort_values("revenue", ascending=False).head(5).copy()
    top5["date"] = top5["date"].astype(str)

    # Rounding and applying thousands separator
    top5["revenue"] = top5["revenue"].apply(lambda x:  f"{x:,.2f}")

    top5.insert(0, "rank", range(1, len(top5) + 1))

    top5.reset_index(drop=True, inplace=True)

    return top5


# ------------------
# Users unification
# ------------------

USER_KEY_FIELDS = ["name", "email", "phone", "address"]


def find_real_user(df: pd.DataFrame) -> int:
    """
    Counts real users. Conditions:
    - User can have one field different (e.g. address, phone)
    - If 2 records differs by only one column they are treated as the same.
    """

    # Picking only existing identifiable columns
    cols = [c for c in USER_KEY_FIELDS if c in df.columns]

    profiles = df[cols].fillna("").apply(tuple, axis=1)

    unique_groups = []

    for p in profiles:
        matched = False

        for group in unique_groups:
            representative = group[0]

            # How many different fields?
            diff = sum(x != y for x, y in zip(p, representative))

            # Only one different field applicable
            if diff <= 1:
                group.append(p)
                matched = True
                break

        if not matched:
            unique_groups.append([p])

    return len(unique_groups)


#---------------
# Authors - sets
#---------------

def normalize_author(author_string: str) -> frozenset:
    """
    Returns frozenset of author names to have one set for different order.
    """
    if not isinstance(author_string, str):
        return frozenset()

    authors = [
        a.strip()
        for a in author_string.replace(",", ";").split(";")
    ]
    return frozenset(authors)


def count_unique_author_sets(df:pd.DataFrame) -> int:
    """
    Counts unique author sets.
    """
    if "author" not in df.columns:
        return 0

    sets_ = df["author"].apply(normalize_author)
    return sets_.nunique()


def most_popular_author_or_set(df: pd.DataFrame) -> str:
    """
    Returns most popular author by number of books sold. If not found - N/A.
    """
    if "author" not in df.columns:
        return "N/A"

    df["author_set"] = df["author"].apply(normalize_author)
    popularity = (df.groupby("author_set")["quantity"].sum().sort_values(ascending=False))
    most_popular_set = popularity.index[0] if not popularity.empty else "N/A"

    return ", ".join(sorted(most_popular_set))


#----------------
# The best client
#----------------

def best_buyer(df: pd.DataFrame) -> list:
    """
    Returns number of all id linked to the best client. Linking through:
    - email
    - phone
    - address
    - name
    """
    cols = ["id", "email", "phone", "address", "name"]
    cols = [c for c in cols if c in df.columns]

    spending = df.groupby(cols, dropna=False)["paid_price"].sum().reset_index()
    best = spending.sort_values("paid_price", ascending=False).iloc[0]

    linked = df[
        (df["email"] == best.get("email")) |
        (df["phone"] == best.get("phone")) |
        (df["address"] == best.get("address")) |
        (df["name"] == best.get("name"))
    ]["id"]

    return [int(x) for x in linked.values]


#--------------
# Compute price
#--------------

def compute_paid_price(df, eur_to_usd=1.2):
    """
    Adds column paid_price = quantity * unit_price (USD).
    """
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    df["paid_price"] = df["quantity"] * df["unit_price"] * eur_to_usd

    return df


#-----------------------
# Main func for analysis
#-----------------------

def analyze(df: pd.DataFrame) -> dict:
    """
    Returns all results for dashboard:
    - Top 5 days
    - Real users number
    - Unique authors set number
    - Most popular author
    - Best client
    - Revenue chart data
    """
    df = compute_paid_price(df)
    daily = compute_daily_revenue(df)
    top5 = get_top5_days(daily)

    results = {
        "daily_revenue": daily,
        "top5_days": top5,
        "unique_users": find_real_user(df),
        "unique_author_sets": count_unique_author_sets(df),
        "most_popular_author_set": most_popular_author_or_set(df),
        "best_buyer_aliases": best_buyer(df),
    }

    return results
