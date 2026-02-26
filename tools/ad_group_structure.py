"""
Tool: analyze_ad_group_structure
Checks keyword count per ad group, thematic relevance, wasted spend, and structural issues.
"""
import pandas as pd
from .utils import load_csv, clean_currency, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/ad_groups.csv", keywords_path: str = "data/keywords.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    ag_col       = find_col(df, 'Ad group', 'Ad Group')
    campaign_col = find_col(df, 'Campaign')
    status_col   = find_col(df, 'Ad group status', 'Status')
    cost_col     = find_col(df, 'Cost', 'Spend')
    conv_col     = find_col(df, 'Conversions', 'Conv.')
    ctr_col      = find_col(df, 'CTR')
    impr_col     = find_col(df, 'Impressions', 'Impr.')
    max_cpc_col  = find_col(df, 'Default max. CPC', 'Default Max CPC', 'Max CPC')

    if not ag_col:
        return {"error": "Could not find 'Ad group' column in ad_groups.csv"}

    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if ctr_col:
        df['_ctr'] = df[ctr_col].apply(clean_percentage_local)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0
    avg_cpa = safe_divide(total_cost, total_conv)

    # Load keywords to get per-ad-group keyword counts
    kw_counts = {}
    try:
        kdf = load_csv(keywords_path)
        kw_ag_col = find_col(kdf, 'Ad group')
        kw_camp_col = find_col(kdf, 'Campaign')
        kw_status_col = find_col(kdf, 'Status', 'Keyword status')

        if kw_ag_col and kw_camp_col:
            active_kdf = kdf
            if kw_status_col:
                active_kdf = kdf[~kdf[kw_status_col].astype(str).str.lower().str.contains('removed|paused', na=False)]
            grp = active_kdf.groupby([kw_camp_col, kw_ag_col]).size()
            kw_counts = {f"{camp}|{ag}": count for (camp, ag), count in grp.items()}
    except Exception:
        pass

    too_many_kws = []
    skags = []
    no_conversion_waste = []
    zero_cpc_issues = []

    for _, row in df.iterrows():
        ag = row.get(ag_col, 'Unknown')
        camp = row.get(campaign_col, '') if campaign_col else ''
        cost = row.get('_cost', 0) or 0
        conv = row.get('_conv', 0) or 0
        impr = row.get('_impr', 0) or 0
        ctr = row.get('_ctr', 0) or 0
        status = str(row.get(status_col, '')).lower() if status_col else ''
        max_cpc = clean_currency_local(row.get(max_cpc_col, 0)) if max_cpc_col else None

        key = f"{camp}|{ag}"
        kw_count = kw_counts.get(key, None)

        # Too many keywords in one ad group
        if kw_count and kw_count > 20:
            too_many_kws.append({"ad_group": ag, "campaign": camp, "keyword_count": kw_count})
            findings.append({
                "severity": "MEDIUM",
                "area": "Ad Group Structure",
                "ad_group": ag,
                "campaign": camp,
                "detail": f"Ad group has {kw_count} keywords. Too many keywords hurt thematic relevance and Quality Score.",
                "recommendation": "Split into smaller, tightly themed ad groups (aim for 5-15 keywords per ad group)."
            })

        # Single keyword ad groups
        if kw_count == 1:
            skags.append({"ad_group": ag, "campaign": camp})
            findings.append({
                "severity": "LOW",
                "area": "Ad Group Structure",
                "ad_group": ag,
                "campaign": camp,
                "detail": "Single-keyword ad group (SKAG). While precise, these create management overhead.",
                "recommendation": "Consider consolidating closely related SKAGs. Ensure at least 2-3 ads are running per group."
            })

        # High spend, zero conversions
        waste_threshold = max(avg_cpa * 3, 50) if avg_cpa > 0 else 75
        if cost > waste_threshold and conv == 0:
            no_conversion_waste.append({"ad_group": ag, "campaign": camp, "cost": round(cost, 2)})
            findings.append({
                "severity": "HIGH",
                "area": "Wasted Ad Group Spend",
                "ad_group": ag,
                "campaign": camp,
                "detail": f"Spent ${cost:.2f} with 0 conversions.",
                "recommendation": "Pause or restructure this ad group. Review keywords, ads, and landing page relevance."
            })

        # Low CTR with significant impressions
        if ctr and ctr < 0.005 and impr > 1000:
            findings.append({
                "severity": "MEDIUM",
                "area": "CTR",
                "ad_group": ag,
                "campaign": camp,
                "detail": f"CTR of {ctr:.2%} with {impr:,.0f} impressions — very low relevance signal.",
                "recommendation": "Rewrite ad copy to be more specific to this ad group's keyword theme."
            })

        # Zero default max CPC with active status
        if max_cpc is not None and max_cpc == 0 and 'enabled' in status:
            zero_cpc_issues.append({"ad_group": ag, "campaign": camp})
            findings.append({
                "severity": "MEDIUM",
                "area": "Bidding",
                "ad_group": ag,
                "campaign": camp,
                "detail": "Default Max CPC is $0 but ad group is enabled. Likely relying on campaign-level bidding.",
                "recommendation": "Set an explicit Max CPC or confirm Smart Bidding strategy is configured at campaign level."
            })

    # Campaigns with too many ad groups
    if campaign_col:
        ag_per_camp = df.groupby(campaign_col)[ag_col].nunique()
        bloated = ag_per_camp[ag_per_camp > 50]
        for camp, count in bloated.items():
            findings.append({
                "severity": "LOW",
                "area": "Account Structure",
                "campaign": camp,
                "detail": f"Campaign has {count} ad groups. This level of fragmentation can be hard to manage.",
                "recommendation": "Consider consolidating similar ad groups. Aim for 10-30 ad groups per campaign."
            })

    summary = (
        f"Analyzed {len(df)} ad groups. "
        f"Ad groups with 20+ keywords: {len(too_many_kws)}. "
        f"SKAGs: {len(skags)}. "
        f"Zero-conversion wasted spend: {len(no_conversion_waste)} ad groups. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "metrics": {
            "total_ad_groups": len(df),
            "ad_groups_too_many_keywords": len(too_many_kws),
            "single_keyword_ad_groups": len(skags),
            "zero_conversion_ad_groups": len(no_conversion_waste),
        }
    }


def clean_percentage_local(val):
    from .utils import clean_percentage
    return clean_percentage(val)


def clean_currency_local(val):
    from .utils import clean_currency
    return clean_currency(val)
