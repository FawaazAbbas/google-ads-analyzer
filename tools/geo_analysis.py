"""
Tool: analyze_geographic_performance
Identifies high-performing regions to bid up and wasteful locations to exclude.
"""
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/geographic.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    campaign_col  = find_col(df, 'Campaign')
    country_col   = find_col(df, 'Country/Territory', 'Country', 'Country territory')
    region_col    = find_col(df, 'Region', 'State')
    city_col      = find_col(df, 'City')
    cost_col      = find_col(df, 'Cost', 'Spend')
    conv_col      = find_col(df, 'Conversions', 'Conv.')
    ctr_col       = find_col(df, 'CTR')
    impr_col      = find_col(df, 'Impressions', 'Impr.')
    clicks_col    = find_col(df, 'Clicks')
    conv_rate_col = find_col(df, 'Conv. rate', 'Conversion rate')
    cost_conv_col = find_col(df, 'Cost / conv.', 'Cost/conv.')

    location_col = city_col or region_col or country_col
    if not location_col:
        return {"error": "Could not find a location column (City/Region/Country) in geographic.csv"}

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

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0
    avg_cpa = safe_divide(total_cost, total_conv)
    avg_conv_rate = df['_conv_rate'].mean() if '_conv_rate' in df else safe_divide(
        total_conv, df['_clicks'].sum() if '_clicks' in df else 0
    )

    # Aggregate by location
    group_col = location_col
    agg = df.groupby(group_col).agg(
        total_cost=('_cost', 'sum'),
        total_conv=('_conv', 'sum'),
        total_clicks=('_clicks', 'sum') if '_clicks' in df.columns else ('_cost', 'count'),
    ).reset_index()

    agg['_cpa'] = agg.apply(lambda r: safe_divide(r['total_cost'], r['total_conv']), axis=1)
    agg['_conv_rate'] = agg.apply(lambda r: safe_divide(r['total_conv'], r.get('total_clicks', 0)), axis=1)

    # Top 5 performers by conversion volume
    top_locations = agg.nlargest(5, 'total_conv')[[group_col, 'total_cost', 'total_conv', '_cpa']].to_dict('records')

    # Top 5 worst CPA (minimum spend)
    significant = agg[agg['total_cost'] > max(avg_cpa * 2, 20)] if avg_cpa > 0 else agg[agg['total_cost'] > 20]
    worst_locations = significant.nlargest(5, '_cpa')[[group_col, 'total_cost', 'total_conv', '_cpa']].to_dict('records')

    exclusion_candidates = []
    bid_up_candidates = []

    for _, row in agg.iterrows():
        location = row.get(group_col, 'Unknown')
        cost = row.get('total_cost', 0) or 0
        conv = row.get('total_conv', 0) or 0
        cpa = row.get('_cpa', 0) or 0
        conv_rate = row.get('_conv_rate', 0) or 0
        cost_share = safe_divide(cost, total_cost)

        # High spend, zero conversions — exclusion candidate
        if cost_share > 0.05 and conv == 0:
            exclusion_candidates.append({
                "location": location,
                "cost_wasted": round(cost, 2),
                "cost_share": f"{cost_share:.1f}%",
            })
            findings.append({
                "severity": "HIGH",
                "area": "Geographic Waste",
                "location": location,
                "detail": f"{location} accounts for {cost_share:.1%} of spend (${cost:.2f}) with 0 conversions.",
                "recommendation": f"Exclude location '{location}' from targeting. Estimated monthly savings: ${cost:.2f}."
            })

        # High CPA vs average
        elif avg_cpa > 0 and cpa > avg_cpa * 2 and cost > avg_cpa:
            findings.append({
                "severity": "MEDIUM",
                "area": "Geographic CPA",
                "location": location,
                "detail": f"{location} CPA ${cpa:.2f} is {cpa/avg_cpa:.1f}x account average ${avg_cpa:.2f}.",
                "recommendation": f"Apply a negative bid adjustment for '{location}' or exclude if no strategic reason to be there."
            })

        # Strong performer with significant volume
        elif avg_conv_rate > 0 and conv_rate > avg_conv_rate * 1.5 and cost > 30:
            bid_up_candidates.append({
                "location": location,
                "conv_rate": f"{conv_rate:.2%}",
                "cpa": round(cpa, 2),
                "recommended_adj": f"+{(conv_rate/avg_conv_rate - 1) * 100:.0f}%",
            })
            findings.append({
                "severity": "MEDIUM",
                "area": "Geographic Opportunity",
                "location": location,
                "detail": f"{location} conv. rate {conv_rate:.2%} is {conv_rate/avg_conv_rate:.1f}x the average. Underinvesting.",
                "recommendation": f"Apply a +{(conv_rate/avg_conv_rate - 1) * 100:.0f}% bid adjustment for '{location}'."
            })

    total_waste = sum(e['cost_wasted'] for e in exclusion_candidates)

    summary = (
        f"Analyzed geographic performance across {len(agg)} locations. "
        f"Total spend: ${total_cost:,.2f}. "
        f"Exclusion candidates: {len(exclusion_candidates)} (${total_waste:,.2f} potential savings). "
        f"Bid-up opportunities: {len(bid_up_candidates)}. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "top_locations_by_conversions": top_locations,
        "worst_locations_by_cpa": worst_locations,
        "exclusion_candidates": exclusion_candidates,
        "bid_up_candidates": bid_up_candidates,
        "metrics": {
            "total_locations": len(agg),
            "account_avg_cpa": round(avg_cpa, 2),
            "total_wasted_spend_on_exclusions": round(total_waste, 2),
        }
    }
