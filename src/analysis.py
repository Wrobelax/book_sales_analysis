"""
Script used for analysis of data.
"""

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
    Returns top 5 days per revenue. Date format: YYY-MM-DD.
    """
    # Normalize to date only
    df_daily = df_daily.copy()
    df_daily["date"] = pd.to_datetime(df_daily["date"], errors="coerce").dt.date

    top5 = df_daily.sort_values("revenue", ascending=False).head(5).copy()

    # Format revenue nicely
    top5["revenue"] = top5["revenue"].apply(lambda x: f"{x:,.2f}")

    # Add rank
    top5.insert(0, "rank", range(1, len(top5) + 1))

    return top5.reset_index(drop=True)


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
    cols = [c for c in USER_KEY_FIELDS if c in df.columns]

    profiles = df[cols].fillna("").astype(str).apply(tuple, axis=1)
    clusters = []

    for p in profiles:
        matched = False
        for group in clusters:
            rep = group["rep"]
            diff = sum(a != b for a, b in zip(p, rep))

            if diff <= 1:
                group["items"].append(p)
                matched = True
                break

        if not matched:
            clusters.append({"rep": p, "items": [p]})

    return len(clusters)


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
        if a.strip()
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

    temp = df["author"].apply(normalize_author)
    valid = temp.apply(lambda s: len(s) > 0)

    if valid.sum() == 0:
        return "N/A"

    popularity = df[valid].groupby(temp[valid])["quantity"].sum()
    best = popularity.sort_values(ascending=False).index[0]

    return ", ".join(sorted(best))


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
    cols = ["user_id", "quantity", "paid_price", "name", "email", "phone", "address"]
    cols = [c for c in cols if c in df.columns]

    df2 = df[cols].copy()
    df2 = df2.fillna("").astype({"paid_price": float})

    profile_cols = [c for c in ["name", "email", "phone", "address"] if c in df2.columns]

    profiles = list(df2.apply(
        lambda r: (int(r["user_id"]), tuple(r[c] for c in profile_cols), float(r["paid_price"])),
        axis=1
    ))

    clusters = []

    for uid, prof, spend in profiles:
        matched = False

        for cl in clusters:
            rep = cl["rep"]
            diff = sum(a != b for a, b in zip(prof, rep))

            if diff <= 1:
                cl["user_ids"].add(uid)
                cl["spend"] += spend
                matched = True
                break

        if not matched:
            clusters.append({"rep": prof, "user_ids": {uid}, "spend": spend})

    if not clusters:
        return []

    best_cluster = max(clusters, key=lambda c: c["spend"])
    return sorted(best_cluster["user_ids"])


#--------------
# Compute price
#--------------

def compute_paid_price(df, eur_to_usd=1.2):
    """
    Adds column paid_price = quantity * unit_price (USD).
    """
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["paid_price"] = df["quantity"] * df["unit_price"]
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
    df = df.copy()

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
