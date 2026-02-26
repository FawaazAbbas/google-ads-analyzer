"""
Tool: analyze_search_terms
Finds irrelevant search terms draining budget, high-performing terms to harvest as
exact match keywords, and themes for new negative keywords.
"""
import pandas as pd
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/search_terms.csv", keywords_path: str = "data/keywords.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    term_col    = find_col(df, 'Search term', 'Search Term')
    campaign_col = find_col(df, 'Campaign')
    adgroup_col  = find_col(df, 'Ad group')
    added_col    = find_col(df, 'Added / Excluded', 'Match type', 'Added/Excluded')
    cost_col     = find_col(df, 'Cost', 'Spend')
    conv_col     = find_col(df, 'Conversions', 'Conv.')
    ctr_col      = find_col(df, 'CTR')
    impr_col     = find_col(df, 'Impressions', 'Impr.')
    clicks_col   = find_col(df, 'Clicks')
    conv_rate_col = find_col(df, 'Conv. rate', 'Conversion rate')

    if not term_col:
        return {"error": "Could not find 'Search term' column in search_terms.csv"}

    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if ctr_col:
        df['_ctr'] = df[ctr_col].apply(clean_percentage)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)
    if clicks_col:
        df['_clicks'] = df[clicks_col].apply(clean_number)
    if conv_rate_col:
        df['_conv_rate'] = df[conv_rate_col].apply(clean_percentage)

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0
    avg_cpa = safe_divide(total_cost, total_conv)
    avg_ctr = df['_ctr'].mean() if '_ctr' in df else 0
    avg_conv_rate = df['_conv_rate'].mean() if '_conv_rate' in df else 0

    # Load existing keywords for comparison
    existing_keywords = set()
    try:
        kdf = load_csv(keywords_path)
        kw_col = find_col(kdf, 'Keyword', 'Keyword text')
        if kw_col:
            existing_keywords = set(kdf[kw_col].astype(str).str.lower().str.strip())
    except Exception:
        pass

    # --- Harvest Candidates ---
    harvest_candidates = []
    already_added = set()
    if added_col:
        already_added = set(
            df[df[added_col].astype(str).str.lower().str.contains('added', na=False)][term_col]
            .astype(str).str.lower().str.strip()
        )

    for _, row in df.iterrows():
        term = str(row.get(term_col, '')).strip()
        term_lower = term.lower()
        cost = row.get('_cost', 0) or 0
        conv = row.get('_conv', 0) or 0
        ctr = row.get('_ctr', 0) or 0
        clicks = row.get('_clicks', 0) or 0
        conv_rate = row.get('_conv_rate', 0) or 0

        if term_lower in already_added or term_lower in existing_keywords:
            continue

        # High converting terms
        if conv >= 2 and (avg_conv_rate == 0 or conv_rate > avg_conv_rate):
            harvest_candidates.append({
                "search_term": term,
                "clicks": int(clicks),
                "conversions": conv,
                "cost": round(cost, 2),
                "recommendation": "Add as Exact Match keyword",
                "campaign": row.get(campaign_col, '') if campaign_col else '',
                "ad_group": row.get(adgroup_col, '') if adgroup_col else '',
            })
            findings.append({
                "severity": "HIGH",
                "area": "Keyword Opportunity",
                "detail": f"Search term '{term}' has {conv} conversions but is NOT an exact match keyword.",
                "recommendation": f"Add '[{term}]' as Exact Match keyword in ad group: {row.get(adgroup_col, '') if adgroup_col else 'Unknown'}"
            })

        # High CTR terms
        elif clicks >= 10 and avg_ctr > 0 and ctr > avg_ctr * 1.5 and conv == 0:
            harvest_candidates.append({
                "search_term": term,
                "clicks": int(clicks),
                "conversions": conv,
                "cost": round(cost, 2),
                "recommendation": "Consider adding as Exact Match keyword (high CTR, needs conversion tracking)",
                "campaign": row.get(campaign_col, '') if campaign_col else '',
                "ad_group": row.get(adgroup_col, '') if adgroup_col else '',
            })

    # --- Negative Keyword Candidates ---
    negative_candidates = []
    waste_threshold = max(avg_cpa * 1.5, 20) if avg_cpa > 0 else 30

    for _, row in df.iterrows():
        term = str(row.get(term_col, '')).strip()
        cost = row.get('_cost', 0) or 0
        conv = row.get('_conv', 0) or 0
        impr = row.get('_impr', 0) or 0
        ctr = row.get('_ctr', 0) or 0

        added_status = str(row.get(added_col, '')).lower() if added_col else ''
        if 'excluded' in added_status:
            continue

        if cost > waste_threshold and conv == 0:
            negative_candidates.append({
                "search_term": term,
                "cost_wasted": round(cost, 2),
                "impressions": int(impr),
                "campaign": row.get(campaign_col, '') if campaign_col else '',
            })
            findings.append({
                "severity": "HIGH",
                "area": "Wasted Search Term Spend",
                "detail": f"Search term '{term}' wasted ${cost:.2f} with 0 conversions.",
                "recommendation": f"Add as negative keyword. Monthly savings estimate: ${cost:.2f}."
            })

        elif impr > 500 and ctr < 0.002:
            findings.append({
                "severity": "LOW",
                "area": "Irrelevant Search Term",
                "detail": f"Search term '{term}' has {impr:,.0f} impressions with only {ctr:.2%} CTR — very low relevance.",
                "recommendation": "Consider adding as negative keyword to improve CTR and Quality Score."
            })

    # --- Negative Keyword Themes ---
    neg_terms = [c['search_term'] for c in negative_candidates]
    theme_counts: dict = {}
    for term in neg_terms:
        words = term.lower().split()
        if words:
            first_word = words[0]
            theme_counts[first_word] = theme_counts.get(first_word, 0) + 1

    top_negative_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    # Waste stats
    wasted_cost = sum(c['cost_wasted'] for c in negative_candidates)
    waste_pct = safe_divide(wasted_cost, total_cost) * 100

    summary = (
        f"Analyzed {len(df)} search terms. "
        f"Total spend: ${total_cost:,.2f}. "
        f"Wasted spend (0 conversions, over threshold): ${wasted_cost:,.2f} ({waste_pct:.1f}% of spend). "
        f"Harvest opportunities: {len(harvest_candidates)}. "
        f"Negative keyword candidates: {len(negative_candidates)}."
    )

    return {
        "summary": summary,
        "findings": findings,
        "harvest_candidates": harvest_candidates[:20],
        "negative_keyword_candidates": negative_candidates[:30],
        "top_negative_themes": [{"word": w, "frequency": c} for w, c in top_negative_themes],
        "metrics": {
            "total_search_terms": len(df),
            "total_spend": round(total_cost, 2),
            "wasted_spend": round(wasted_cost, 2),
            "waste_percentage": round(waste_pct, 1),
        }
    }
