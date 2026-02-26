"""
Tool: analyze_devices
Compares Mobile, Desktop, Tablet performance and recommends bid adjustments.
"""
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/devices.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    device_col    = find_col(df, 'Device')
    campaign_col  = find_col(df, 'Campaign')
    cost_col      = find_col(df, 'Cost', 'Spend')
    conv_col      = find_col(df, 'Conversions', 'Conv.')
    ctr_col       = find_col(df, 'CTR')
    impr_col      = find_col(df, 'Impressions', 'Impr.')
    clicks_col    = find_col(df, 'Clicks')
    conv_rate_col = find_col(df, 'Conv. rate', 'Conversion rate')
    cost_conv_col = find_col(df, 'Cost / conv.', 'Cost/conv.')
    bid_adj_col   = find_col(df, 'Bid adjustment', 'Bid Adjustment')

    if not device_col:
        return {"error": "Could not find 'Device' column in devices.csv"}

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
    if cost_conv_col:
        df['_cpa'] = df[cost_conv_col].apply(clean_currency)
    if bid_adj_col:
        df['_bid_adj'] = df[bid_adj_col].apply(clean_percentage)

    # Aggregate by device
    agg = df.groupby(device_col).agg(
        total_cost=('_cost', 'sum'),
        total_conv=('_conv', 'sum'),
        total_impr=('_impr', 'sum'),
        total_clicks=('_clicks', 'sum'),
    ).reset_index() if '_cost' in df else df.groupby(device_col).size().reset_index()

    # Compute derived metrics
    device_metrics = []
    account_total_cost = agg['total_cost'].sum() if 'total_cost' in agg.columns else 0
    account_total_conv = agg['total_conv'].sum() if 'total_conv' in agg.columns else 0
    account_avg_cpa = safe_divide(account_total_cost, account_total_conv)
    account_avg_conv_rate = safe_divide(account_total_conv, agg['total_clicks'].sum()) if 'total_clicks' in agg.columns else 0

    bid_recommendations = {}

    for _, row in agg.iterrows():
        device = row[device_col]
        cost = row.get('total_cost', 0) or 0
        conv = row.get('total_conv', 0) or 0
        impr = row.get('total_impr', 0) or 0
        clicks = row.get('total_clicks', 0) or 0

        cpa = safe_divide(cost, conv)
        conv_rate = safe_divide(conv, clicks)
        ctr = safe_divide(clicks, impr)
        cost_share = safe_divide(cost, account_total_cost) * 100

        # Recommended bid adjustment based on relative conv_rate
        if account_avg_conv_rate > 0 and conv_rate > 0:
            rec_adj = ((conv_rate / account_avg_conv_rate) - 1) * 100
            rec_adj = max(-90, min(900, rec_adj))  # Google's limits
        else:
            rec_adj = 0

        current_adj = df[df[device_col] == device]['_bid_adj'].mean() if '_bid_adj' in df else 0
        current_adj_pct = (current_adj or 0) * 100

        bid_recommendations[device] = {
            "current_adjustment": f"{current_adj_pct:+.0f}%",
            "recommended_adjustment": f"{rec_adj:+.0f}%",
        }

        device_metrics.append({
            "device": device,
            "cost": round(cost, 2),
            "cost_share": f"{cost_share:.1f}%",
            "conversions": round(conv, 1),
            "cpa": round(cpa, 2),
            "conv_rate": f"{conv_rate:.2%}",
            "ctr": f"{ctr:.2%}",
            "recommended_bid_adj": f"{rec_adj:+.0f}%",
        })

        # Flag devices needing bid changes
        if account_avg_cpa > 0:
            if cpa > account_avg_cpa * 1.5 and cost > 50 and current_adj_pct >= 0:
                findings.append({
                    "severity": "HIGH",
                    "area": "Device Bid",
                    "device": device,
                    "detail": f"{device} CPA ${cpa:.2f} is {cpa/account_avg_cpa:.1f}x the account average. Current bid adj: {current_adj_pct:+.0f}%.",
                    "recommendation": f"Apply a {rec_adj:+.0f}% bid adjustment for {device}."
                })
            elif cpa < account_avg_cpa * 0.7 and cost > 50 and current_adj_pct <= 0:
                findings.append({
                    "severity": "HIGH",
                    "area": "Device Bid",
                    "device": device,
                    "detail": f"{device} CPA ${cpa:.2f} is significantly better than account avg ${account_avg_cpa:.2f}. Missing opportunity.",
                    "recommendation": f"Increase {device} bids by {rec_adj:+.0f}% to capture more of this efficient traffic."
                })

        # Mobile-specific check
        if 'mobile' in str(device).lower():
            desktop_row = agg[agg[device_col].astype(str).str.lower().str.contains('desktop', na=False)]
            if not desktop_row.empty:
                desktop_conv_rate = safe_divide(
                    desktop_row['total_conv'].values[0],
                    desktop_row['total_clicks'].values[0]
                ) if 'total_clicks' in agg.columns else 0
                if desktop_conv_rate > 0 and conv_rate < desktop_conv_rate * 0.4:
                    findings.append({
                        "severity": "HIGH",
                        "area": "Mobile Experience",
                        "device": "Mobile",
                        "detail": f"Mobile conv. rate ({conv_rate:.2%}) is less than 40% of Desktop ({desktop_conv_rate:.2%}). Significant mobile UX issue.",
                        "recommendation": "Audit mobile landing page speed and UX. Consider mobile-specific landing pages or reducing mobile bids."
                    })

    summary = (
        f"Analyzed device performance. "
        f"Account avg CPA: ${account_avg_cpa:.2f}. "
        f"Bid recommendations generated for {len(bid_recommendations)} devices. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "device_performance_table": device_metrics,
        "bid_adjustment_recommendations": bid_recommendations,
        "metrics": {
            "account_avg_cpa": round(account_avg_cpa, 2),
            "account_avg_conv_rate": round(account_avg_conv_rate, 4),
        }
    }
