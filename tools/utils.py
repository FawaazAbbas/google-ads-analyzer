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
    """
    Load a Google Ads CSV export robustly.

    Handles the common messiness of real Google Ads exports:
    - Report title rows at the top (e.g. "Campaign performance report")
    - BOM characters (utf-8-sig encoding)
    - Non-comma delimiters (semicolons, tabs — common in non-English locales)
    - Summary/total footer rows at the bottom
    - Trailing empty rows
    """
    import csv as csv_module

    # Read raw lines to inspect structure before parsing
    encodings = ['utf-8-sig', 'utf-8', 'latin-1']
    lines = None
    used_encoding = 'utf-8-sig'
    for enc in encodings:
        try:
            with open(path, 'r', encoding=enc) as f:
                lines = f.read().splitlines()
            used_encoding = enc
            break
        except Exception:
            continue

    if lines is None:
        raise ValueError("Could not read file: {}".format(path))

    # Find the first line that looks like a real header row:
    # it must have at least 3 fields when split by a common delimiter.
    header_idx = 0
    for i, line in enumerate(lines):
        stripped = line.strip().strip('\ufeff')
        if not stripped:
            continue
        # Count fields for each candidate delimiter
        for delim in [',', ';', '\t', '|']:
            parts = stripped.split(delim)
            if len(parts) >= 3:
                header_idx = i
                break
        else:
            continue
        break

    # Detect the delimiter from the header + a few data rows
    sample_lines = [l for l in lines[header_idx:header_idx + 6] if l.strip()]
    sample = '\n'.join(sample_lines)
    sep = ','
    try:
        dialect = csv_module.Sniffer().sniff(sample, delimiters=',;\t|')
        sep = dialect.delimiter
    except Exception:
        # Fallback: pick the delimiter that produces the most columns in the header
        header = lines[header_idx] if header_idx < len(lines) else ''
        best = max([',', ';', '\t', '|'], key=lambda d: len(header.split(d)))
        sep = best

    # Load the CSV, skipping title rows and footer rows
    try:
        df = pd.read_csv(
            path,
            sep=sep,
            skiprows=header_idx,
            skipfooter=2,
            engine='python',
            encoding=used_encoding,
            thousands=',',
            on_bad_lines='skip',
        )
    except TypeError:
        # pandas < 1.3 uses error_bad_lines instead of on_bad_lines
        df = pd.read_csv(
            path,
            sep=sep,
            skiprows=header_idx,
            skipfooter=2,
            engine='python',
            encoding=used_encoding,
            thousands=',',
            error_bad_lines=False,
        )

    # Clean up
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how='all')                          # drop fully-empty rows
    df = df[~df.iloc[:, 0].astype(str).str.lower()    # drop "Total" footer rows
              .str.startswith('total')]

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


# Maps classified type key → canonical filename used by the analysis tools
TYPE_TO_FILENAME = {
    "campaigns":    "campaigns.csv",
    "keywords":     "keywords.csv",
    "search_terms": "search_terms.csv",
    "ads":          "ads.csv",
    "ad_groups":    "ad_groups.csv",
    "devices":      "devices.csv",
    "audiences":    "audiences.csv",
    "extensions":   "extensions.csv",
    "geographic":   "geographic.csv",
    "time_of_day":  "time_of_day.csv",
    "day_of_week":  "day_of_week.csv",
}

# Human-readable labels for each type key
TYPE_LABELS = {
    "campaigns":    "Campaign Performance",
    "keywords":     "Keywords & Quality Score",
    "search_terms": "Search Terms",
    "ads":          "Ad Creatives",
    "ad_groups":    "Ad Group Structure",
    "devices":      "Device Performance",
    "audiences":    "Audiences",
    "extensions":   "Ad Extensions",
    "geographic":   "Geographic Performance",
    "time_of_day":  "Hour of Day Performance",
    "day_of_week":  "Day of Week Performance",
}

# Primary dimension columns that appear as the FIRST column in each report type.
# Ordered from most specific to most generic to avoid false matches.
_FIRST_COL_MAP = [
    ("search term",       "search_terms"),
    ("hour of day",       "time_of_day"),
    ("day of week",       "day_of_week"),
    ("country/territory", "geographic"),
    ("asset type",        "extensions"),
    ("audience segment",  "audiences"),
    ("audience",          "audiences"),
    ("device",            "devices"),
    ("ad group",          "ad_groups"),
    ("keyword",           "keywords"),
    ("campaign",          "campaigns"),
    ("city",              "geographic"),
    ("region",            "geographic"),
    ("location",          "geographic"),
    ("ad",                "ads"),
]

# Weighted column signatures used when the first-column check is inconclusive.
# Higher weights = stronger signal for that report type.
_SIGNATURES = {
    "campaigns": {
        "budget": 6,
        "search impr. share": 8,
        "impr. share": 5,
        "cost / conv.": 3,
    },
    "keywords": {
        "quality score": 10,
        "ad relevance": 8,
        "landing page exp.": 8,
        "exp. ctr": 6,
        "match type": 3,
    },
    "search_terms": {
        "search term": 12,
        "added/excluded": 8,
    },
    "ads": {
        "headline 1": 10,
        "ad strength": 10,
        "description 1": 8,
        "final url": 4,
    },
    "ad_groups": {
        "default max. cpc": 10,
        "ad group": 5,
    },
    "devices": {
        "device": 10,
    },
    "audiences": {
        "audience": 8,
        "bid adj.": 5,
    },
    "extensions": {
        "asset type": 10,
        "association level": 10,
        "extension": 5,
    },
    "geographic": {
        "country/territory": 12,
        "city": 8,
        "region": 6,
    },
    "time_of_day": {
        "hour of day": 14,
    },
    "day_of_week": {
        "day of week": 14,
        "day": 5,
    },
}


def classify_csv(path):
    """
    Inspect a CSV's column headers and return its Google Ads report type key, or None.

    Strategy:
    1. Load just enough to read headers via load_csv.
    2. Check the first column (the primary dimension) for a high-confidence match.
    3. Fall back to weighted signature scoring across all columns.

    Returns one of: "campaigns", "keywords", "search_terms", "ads", "ad_groups",
    "devices", "audiences", "extensions", "geographic", "time_of_day", "day_of_week",
    or None if the file cannot be identified.
    """
    try:
        df = load_csv(path)
    except Exception:
        return None

    if df.empty or len(df.columns) < 2:
        return None

    cols_lower = [c.lower().strip() for c in df.columns]
    first_col = cols_lower[0] if cols_lower else ""

    # Step 1: match primary dimension column (first column) — high confidence
    for fragment, type_key in _FIRST_COL_MAP:
        if first_col == fragment or first_col.startswith(fragment + " "):
            return type_key

    # Step 2: weighted signature scoring across all columns
    scores = {}
    for type_key, sig in _SIGNATURES.items():
        score = 0
        for fragment, weight in sig.items():
            if any(fragment in col for col in cols_lower):
                score += weight
        scores[type_key] = score

    if not scores:
        return None

    best_type = max(scores, key=scores.get)
    best_score = scores[best_type]

    return best_type if best_score >= 5 else None
