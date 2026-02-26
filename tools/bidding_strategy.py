"""
Tool: analyze_bidding_strategies
Reviews bidding strategy suitability based on conversion volume and performance alignment.
"""
from .utils import load_csv, clean_currency, clean_number, clean_percentage, safe_divide, find_col


def analyze(data_path: str = "data/campaigns.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    name_col     = find_col(df, 'Campaign', 'Campaign name')
    cost_col     = find_col(df, 'Cost', 'Spend')
    conv_col     = find_col(df, 'Conversions', 'Conv.')
    conv_val_col = find_col(df, 'Conversion value', 'Conv. value')
    lost_rank    = find_col(df, 'Search lost IS (rank)', 'Search Lost IS (rank)')
    cost_conv    = find_col(df, 'Cost / conv.', 'Cost/conv.')
    impr_col     = find_col(df, 'Impressions', 'Impr.')
    conv_rate    = find_col(df, 'Conv. rate', 'Conversion rate')
    type_col     = find_col(df, 'Campaign type', 'Type')

    if not name_col:
        return {"error": "Could not find Campaign column in campaigns.csv"}

    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if conv_val_col:
        df['_conv_val'] = df[conv_val_col].apply(clean_currency)
    if lost_rank:
        df['_lost_rank'] = df[lost_rank].apply(clean_percentage)
    if cost_conv:
        df['_cpa'] = df[cost_conv].apply(clean_currency)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)
    if conv_rate:
        df['_conv_rate'] = df[conv_rate].apply(clean_percentage)

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0
    account_avg_cpa = safe_divide(total_cost, total_conv)

    smart_bidding_recommendations = []

    for _, row in df.iterrows():
        name = row.get(name_col, 'Unknown')
        cost = row.get('_cost', 0) or 0
        conv = row.get('_conv', 0) or 0
        conv_val = row.get('_conv_val', 0) or 0
        lost_r = row.get('_lost_rank') or 0
        cpa = row.get('_cpa', 0) or 0
        impr = row.get('_impr', 0) or 0

        # Insufficient data for Smart Bidding
        if conv < 30 and cost > 50:
            findings.append({
                "severity": "MEDIUM",
                "area": "Smart Bidding Readiness",
                "campaign": name,
                "detail": f"Only {conv:.0f} conversions in the period. Smart Bidding needs 30-50+ conversions/month to optimize effectively.",
                "recommendation": "Use Manual CPC or Maximize Clicks to build conversion data before switching to Target CPA or Target ROAS."
            })
            smart_bidding_recommendations.append({
                "campaign": name,
                "conversions": conv,
                "recommended_strategy": "Manual CPC or Maximize Clicks (until 30+ conv/month)"
            })

        # High impression share loss due to rank — under-investing
        if lost_r and lost_r > 0.25 and conv > 0:
            roas = safe_divide(conv_val, cost) if conv_val > 0 else None
            cpa_str = f"${cpa:.2f}" if cpa else "N/A"
            findings.append({
                "severity": "HIGH",
                "area": "Bidding — Under-Investing",
                "campaign": name,
                "detail": f"Losing {lost_r:.0%} of impressions to low ad rank despite {conv:.0f} conversions. CPA: {cpa_str}.",
                "recommendation": "Increase bids or switch to a Smart Bidding strategy to compete for more auctions."
            })

        # Negative ROAS despite Smart Bidding (inferred)
        if conv_val > 0 and cost > 0:
            roas = safe_divide(conv_val, cost)
            if roas < 1.0 and cost > 100:
                findings.append({
                    "severity": "HIGH",
                    "area": "ROAS Below Break-Even",
                    "campaign": name,
                    "detail": f"ROAS is {roas:.2f} — returning less in conversion value than spent. Cost: ${cost:.2f}, Value: ${conv_val:.2f}.",
                    "recommendation": "If using Target ROAS, increase the target. If Manual CPC, reduce bids on low-performing keywords."
                })

        # High CPA vs account average
        if account_avg_cpa > 0 and cpa > account_avg_cpa * 2.5 and cost > 50:
            findings.append({
                "severity": "HIGH",
                "area": "CPA — Bidding Misalignment",
                "campaign": name,
                "detail": f"CPA ${cpa:.2f} is {cpa/account_avg_cpa:.1f}x the account average ${account_avg_cpa:.2f}.",
                "recommendation": "If using Target CPA, set a more realistic target. Review keyword quality and landing page relevance."
            })

        # Zero spend — possibly incorrect bidding config
        if impr == 0 and cost == 0 and row.get('_conv_rate') is not None:
            findings.append({
                "severity": "MEDIUM",
                "area": "Delivery",
                "campaign": name,
                "detail": "Campaign has zero impressions. Possible bidding or budget issue preventing delivery.",
                "recommendation": "Check bid strategy settings, budget, and ad approval status."
            })

    findings.append({
        "severity": "INFO",
        "area": "Smart Bidding Note",
        "campaign": "All campaigns",
        "detail": "Detailed bidding strategy types (Target CPA, Target ROAS, etc.) are not available in standard CSV exports.",
        "recommendation": "For full bidding strategy audit, export the Campaign Settings report or use the Google Ads API."
    })

    summary = (
        f"Analyzed bidding effectiveness across {len(df)} campaigns. "
        f"Campaigns with insufficient Smart Bidding data (<30 conv): {len(smart_bidding_recommendations)}. "
        f"Account avg CPA: ${account_avg_cpa:.2f}. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "smart_bidding_readiness": smart_bidding_recommendations,
        "metrics": {
            "account_avg_cpa": round(account_avg_cpa, 2),
            "total_conversions": round(total_conv, 1),
            "total_cost": round(total_cost, 2),
        }
    }
