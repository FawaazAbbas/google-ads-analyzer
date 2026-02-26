"""
Shared preprocessing utilities for all Google Ads analysis tools.
Google Ads CSV exports are messy — percentages as strings, currencies with symbols, etc.
"""
import pandas as pd


def clean_percentage(val):
    """Convert '23.4%', '< 10%', '> 90%' etc. to a float (0.234)."""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val) / 100 if float(val) > 1 else float(val)
    val = str(val).replace('%', '').replace('< ', '').replace('>', '').replace('--', '').strip()
    try:
        return float(val) / 100
    except (ValueError, TypeError):
        return None


def clean_currency(val):
    """Convert '$1,234.56' or '1234.56' to a float."""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    val = str(val).replace('$', '').replace(',', '').replace('--', '').strip()
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def clean_number(val):
    """Convert '1,234' or '1234' to a float."""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    val = str(val).replace(',', '').replace('--', '').strip()
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def load_csv(path: str) -> pd.DataFrame:
    """Load a Google Ads CSV export, skipping the summary footer rows."""
    try:
        df = pd.read_csv(path, skipfooter=2, engine='python')
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception:
        # Try without skipfooter
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        return df


def safe_divide(numerator, denominator, default=0.0):
    """Safe division returning default when denominator is zero."""
    if denominator == 0 or pd.isna(denominator):
        return default
    return numerator / denominator


def compute_benchmarks(df: pd.DataFrame) -> dict:
    """
    Compute account-level benchmark metrics from a campaigns or keywords dataframe.
    Returns a dict of avg_cpa, avg_ctr, avg_conv_rate, total_cost, total_conversions.
    """
    benchmarks = {}

    cost_col = next((c for c in df.columns if 'cost' in c.lower() and 'conv' not in c.lower()), None)
    conv_col = next((c for c in df.columns if c.lower() in ['conversions', 'conv.']), None)
    ctr_col = next((c for c in df.columns if 'ctr' in c.lower()), None)

    if cost_col:
        costs = df[cost_col].apply(clean_currency).dropna()
        benchmarks['total_cost'] = costs.sum()

    if conv_col:
        convs = df[conv_col].apply(clean_number).dropna()
        benchmarks['total_conversions'] = convs.sum()

    if cost_col and conv_col:
        total_cost = benchmarks.get('total_cost', 0)
        total_conv = benchmarks.get('total_conversions', 0)
        benchmarks['avg_cpa'] = safe_divide(total_cost, total_conv)

    if ctr_col:
        ctrs = df[ctr_col].apply(clean_percentage).dropna()
        benchmarks['avg_ctr'] = ctrs.mean()

    conv_rate_col = next((c for c in df.columns if 'conv. rate' in c.lower() or 'conv rate' in c.lower()), None)
    if conv_rate_col:
        rates = df[conv_rate_col].apply(clean_percentage).dropna()
        benchmarks['avg_conv_rate'] = rates.mean()

    return benchmarks


def find_col(df: pd.DataFrame, *candidates):
    """Find the first matching column name (case-insensitive) from a list of candidates."""
    lower_cols = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_cols:
            return lower_cols[candidate.lower()]
    return None
