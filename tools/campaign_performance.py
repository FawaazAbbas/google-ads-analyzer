"""
Tool: analyze_campaign_performance
Analyzes campaign-level KPIs, budget utilization, impression share, ROAS, and CPA.
"""
from .utils import load_csv, clean_percentage, clean_currency, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/campaigns.csv") -> dict:
    df = load_csv(data_path)

    findings = []
    metrics = {}

    # --- Normalize key columns ---
    cost_col     = find_col(df, 'Cost', 'Spend')
    conv_col     = find_col(df, 'Conversions', 'Conv.')
    conv_val_col = find_col(df, 'Conversion value', 'Conv. value', 'All conv. value')
    ctr_col      = find_col(df, 'CTR')
    impr_col     = find_col(df, 'Impressions', 'Impr.')
    clicks_col   = find_col(df, 'Clicks')
    budget_col   = find_col(df, 'Budget', 'Daily budget')
    lost_budget  = find_col(df, 'Search lost IS (budget)', 'Search Lost IS (budget)')
    lost_rank    = find_col(df, 'Search lost IS (rank)', 'Search Lost IS (rank)')
    status_col   = find_col(df, 'Campaign status', 'Status')
    cpc_col      = find_col(df, 'Avg. CPC', 'Avg CPC')
    cost_conv    = find_col(df, 'Cost / conv.', 'Cost/conv.', 'CPA')
    name_col     = find_col(df, 'Campaign', 'Campaign name')

    if not name_col:
        return {"error": "Could not find Campaign column in campaigns.csv"}

    # Clean numeric columns
    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if conv_val_col:
        df['_conv_val'] = df[conv_val_col].apply(clean_currency)
    if ctr_col:
        df['_ctr'] = df[ctr_col].apply(clean_percentage)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)
    if budget_col:
        df['_budget'] = df[budget_col].apply(clean_currency)
    if lost_budget:
        df['_lost_budget'] = df[lost_budget].apply(clean_percentage)
    if lost_rank:
        df['_lost_rank'] = df[lost_rank].apply(clean_percentage)
    if cost_conv:
        df['_cost_conv'] = df[cost_conv].apply(clean_currency)

    # --- Account-level benchmarks ---
    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0
    total_conv_val = df['_conv_val'].sum() if '_conv_val' in df else 0
    avg_cpa = safe_divide(total_cost, total_conv)
    avg_ctr = df['_ctr'].mean() if '_ctr' in df else 0
    account_roas = safe_divide(total_conv_val, total_cost)

    metrics = {
        "total_cost": round(total_cost, 2),
        "total_conversions": round(total_conv, 1),
        "account_avg_cpa": round(avg_cpa, 2),
        "account_avg_ctr": round(avg_ctr, 4),
        "account_roas": round(account_roas, 2),
    }

    # --- Campaign-level checks ---
    for _, row in df.iterrows():
        name = row.get(name_col, 'Unknown')
        cost = row.get('_cost', 0) or 0
        conv = row.get('_conv', 0) or 0
        impr = row.get('_impr', 0) or 0
        budget = row.get('_budget', 0) or 0
        ctr = row.get('_ctr', 0) or 0
        cpa = row.get('_cost_conv', 0) or 0
        lost_b = row.get('_lost_budget') or 0
        lost_r = row.get('_lost_rank') or 0
        status = str(row.get(status_col, '')).lower() if status_col else ''

        # Budget-capped campaigns
        if lost_b and lost_b > 0.10:
            findings.append({
                "severity": "HIGH",
                "area": "Budget",
                "campaign": name,
                "detail": f"Losing {lost_b:.0%} of impressions due to budget cap.",
                "recommendation": f"Increase daily budget or improve Quality Score to recapture lost impressions."
            })

        # Rank-limited campaigns
        if lost_r and lost_r > 0.20:
            findings.append({
                "severity": "HIGH",
                "area": "Impression Share (Rank)",
                "campaign": name,
                "detail": f"Losing {lost_r:.0%} of impressions due to low ad rank.",
                "recommendation": "Improve Quality Score or increase bids to improve ad rank."
            })

        # High CPA vs account average
        if avg_cpa > 0 and cpa > avg_cpa * 2 and cost > 50:
            findings.append({
                "severity": "HIGH",
                "area": "CPA",
                "campaign": name,
                "detail": f"CPA of ${cpa:.2f} is {cpa/avg_cpa:.1f}x the account average of ${avg_cpa:.2f}.",
                "recommendation": "Review keyword relevance, bidding strategy, and landing page quality."
            })

        # Negative ROAS (spending more than returning)
        if '_conv_val' in df and conv > 0:
            conv_val = row.get('_conv_val', 0) or 0
            roas = safe_divide(conv_val, cost)
            if roas < 1.0 and cost > 100:
                findings.append({
                    "severity": "HIGH",
                    "area": "ROAS",
                    "campaign": name,
                    "detail": f"ROAS of {roas:.2f} — spending more than returning. Spent ${cost:.2f}, returned ${conv_val:.2f}.",
                    "recommendation": "Reduce bids, tighten targeting, or pause campaign for review."
                })

        # Low CTR on search campaigns
        if ctr and ctr < 0.005 and impr > 1000:
            findings.append({
                "severity": "MEDIUM",
                "area": "CTR",
                "campaign": name,
                "detail": f"CTR of {ctr:.2%} is very low with {impr:,.0f} impressions. Ads are not resonating.",
                "recommendation": "Review ad copy relevance, add more specific headlines, check keyword-to-ad alignment."
            })

        # Active campaign with zero impressions
        if status == 'enabled' and impr == 0:
            findings.append({
                "severity": "MEDIUM",
                "area": "Delivery",
                "campaign": name,
                "detail": "Campaign is enabled but received zero impressions in the report period.",
                "recommendation": "Check for billing issues, policy disapprovals, targeting too narrow, or budget too low."
            })

        # Under-pacing (severely under-spending budget)
        if budget and budget > 0 and cost > 0:
            daily_avg = cost / 30
            utilization = safe_divide(daily_avg, budget)
            if utilization < 0.20 and cost > 10:
                findings.append({
                    "severity": "LOW",
                    "area": "Budget Utilization",
                    "campaign": name,
                    "detail": f"Only using {utilization:.0%} of daily budget (${daily_avg:.2f} avg vs ${budget:.2f} budget).",
                    "recommendation": "Reduce budget to match actual spend, or broaden targeting to increase delivery."
                })

    summary = (
        f"Analyzed {len(df)} campaigns. "
        f"Total spend: ${total_cost:,.2f}. "
        f"Total conversions: {total_conv:,.0f}. "
        f"Account avg CPA: ${avg_cpa:.2f}. "
        f"Found {len(findings)} issues."
    )

    return {"summary": summary, "findings": findings, "metrics": metrics}
