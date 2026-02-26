"""
Tool: analyze_audiences
Analyzes audience segment performance, bid adjustment gaps, and remarketing coverage.
"""
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/audiences.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    campaign_col  = find_col(df, 'Campaign')
    adgroup_col   = find_col(df, 'Ad group')
    audience_col  = find_col(df, 'Audience segment', 'Audience', 'Audience name')
    type_col      = find_col(df, 'Audience type', 'Type')
    bid_adj_col   = find_col(df, 'Bid adjustment', 'Bid Adjustment')
    cost_col      = find_col(df, 'Cost', 'Spend')
    conv_col      = find_col(df, 'Conversions', 'Conv.')
    ctr_col       = find_col(df, 'CTR')
    cost_conv_col = find_col(df, 'Cost / conv.', 'Cost/conv.')
    conv_rate_col = find_col(df, 'Conv. rate', 'Conversion rate')
    impr_col      = find_col(df, 'Impressions', 'Impr.')

    if not audience_col:
        return {"error": "Could not find audience column in audiences.csv"}

    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if ctr_col:
        df['_ctr'] = df[ctr_col].apply(clean_percentage)
    if cost_conv_col:
        df['_cpa'] = df[cost_conv_col].apply(clean_currency)
    if conv_rate_col:
        df['_conv_rate'] = df[conv_rate_col].apply(clean_percentage)
    if bid_adj_col:
        df['_bid_adj'] = df[bid_adj_col].apply(clean_percentage)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0
    avg_cpa = safe_divide(total_cost, total_conv)
    avg_conv_rate = df['_conv_rate'].mean() if '_conv_rate' in df else 0

    # --- Remarketing Coverage ---
    has_remarketing = False
    if type_col:
        has_remarketing = df[type_col].astype(str).str.lower().str.contains('remarketing|retargeting', na=False).any()
    elif audience_col:
        has_remarketing = df[audience_col].astype(str).str.lower().str.contains('remarketing|retargeting|website visitor', na=False).any()

    if not has_remarketing:
        findings.append({
            "severity": "HIGH",
            "area": "Remarketing",
            "audience": "All audiences",
            "detail": "No remarketing audiences detected in this account.",
            "recommendation": "Set up Google Ads remarketing tags and create audience lists for website visitors, cart abandoners, and past converters."
        })

    # --- Per-Audience Analysis ---
    for _, row in df.iterrows():
        audience = str(row.get(audience_col, 'Unknown'))
        cost = row.get('_cost', 0) or 0
        conv = row.get('_conv', 0) or 0
        cpa = row.get('_cpa', 0) or 0
        conv_rate = row.get('_conv_rate', 0) or 0
        bid_adj = row.get('_bid_adj', 0) or 0
        impr = row.get('_impr', 0) or 0
        camp = row.get(campaign_col, '') if campaign_col else ''

        # High performer with no bid adjustment
        if avg_conv_rate > 0 and conv_rate > avg_conv_rate * 1.3 and abs(bid_adj) < 0.05 and cost > 20:
            findings.append({
                "severity": "HIGH",
                "area": "Audience Bid Adjustment",
                "audience": audience,
                "campaign": camp,
                "detail": f"Audience conv. rate {conv_rate:.2%} is {conv_rate/avg_conv_rate:.1f}x the average. No bid adjustment set.",
                "recommendation": f"Add a +{(conv_rate/avg_conv_rate - 1) * 100:.0f}% bid adjustment to prioritize this audience."
            })

        # Overspending on poor audience
        if avg_cpa > 0 and cpa > avg_cpa * 2 and cost > 30 and bid_adj >= 0:
            findings.append({
                "severity": "MEDIUM",
                "area": "Audience Spend",
                "audience": audience,
                "campaign": camp,
                "detail": f"CPA ${cpa:.2f} is {cpa/avg_cpa:.1f}x the account average. Currently bidding {bid_adj:+.0%}.",
                "recommendation": f"Apply a negative bid adjustment of -{((cpa/avg_cpa) - 1) * 50:.0f}% to reduce waste on this audience."
            })

        # Zero conversions with meaningful spend
        if cost > 50 and conv == 0:
            findings.append({
                "severity": "MEDIUM",
                "area": "Audience Waste",
                "audience": audience,
                "campaign": camp,
                "detail": f"Spent ${cost:.2f} with 0 conversions.",
                "recommendation": "Consider excluding this audience or applying a significant negative bid adjustment."
            })

        # Small remarketing list
        is_remarketing = False
        if type_col:
            is_remarketing = 'remarketing' in str(row.get(type_col, '')).lower()
        if is_remarketing and impr < 100:
            findings.append({
                "severity": "LOW",
                "area": "Remarketing List Size",
                "audience": audience,
                "campaign": camp,
                "detail": f"Remarketing audience has only {impr:.0f} impressions — list is likely too small.",
                "recommendation": "Grow your remarketing list by expanding eligibility windows or adding more audience sources (e.g., YouTube viewers, email lists)."
            })

    summary = (
        f"Analyzed {len(df)} audience segments. "
        f"Remarketing found: {'Yes' if has_remarketing else 'No — CRITICAL'}. "
        f"Account avg CPA: ${avg_cpa:.2f}. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "metrics": {
            "total_audience_segments": len(df),
            "remarketing_configured": has_remarketing,
            "avg_cpa": round(avg_cpa, 2),
            "avg_conv_rate": round(avg_conv_rate, 4),
        }
    }
