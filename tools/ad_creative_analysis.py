"""
Tool: analyze_ad_creatives
Evaluates ad strength ratings, A/B test coverage, and flags underperforming ads.
"""
import pandas as pd
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/ads.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    ad_id_col    = find_col(df, 'Ad ID', 'Ad id')
    campaign_col = find_col(df, 'Campaign')
    adgroup_col  = find_col(df, 'Ad group')
    status_col   = find_col(df, 'Status', 'Ad status')
    strength_col = find_col(df, 'Ad strength', 'Strength')
    type_col     = find_col(df, 'Ad type', 'Type')
    url_col      = find_col(df, 'Final URL', 'Final Url', 'Landing page')
    cost_col     = find_col(df, 'Cost', 'Spend')
    conv_col     = find_col(df, 'Conversions', 'Conv.')
    ctr_col      = find_col(df, 'CTR')
    impr_col     = find_col(df, 'Impressions', 'Impr.')

    if not adgroup_col:
        return {"error": "Could not find 'Ad group' column in ads.csv"}

    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if ctr_col:
        df['_ctr'] = df[ctr_col].apply(clean_percentage)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)

    # Only look at enabled/active ads for most checks
    active_df = df
    if status_col:
        active_df = df[df[status_col].astype(str).str.lower().str.contains('enabled|active', na=False)]

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0

    # ---- Ad Strength Audit ----
    strength_summary = {}
    if strength_col:
        strength_counts = active_df[strength_col].value_counts()
        strength_summary = strength_counts.to_dict()

        poor_ads = active_df[
            active_df[strength_col].astype(str).str.lower().str.contains('poor|average', na=False)
        ]
        for _, row in poor_ads.iterrows():
            strength = row.get(strength_col, 'Unknown')
            ag = row.get(adgroup_col, 'Unknown')
            camp = row.get(campaign_col, '') if campaign_col else ''
            findings.append({
                "severity": "MEDIUM",
                "area": "Ad Strength",
                "ad_group": ag,
                "campaign": camp,
                "detail": f"Ad strength is '{strength}'. Google limits impression share for low-strength ads.",
                "recommendation": "Add more unique headlines, pin fewer assets, improve headline diversity."
            })

    # ---- A/B Test Coverage ----
    if campaign_col:
        group_key = [campaign_col, adgroup_col]
        active_counts = active_df.groupby(group_key).size().reset_index(name='active_ad_count')

        no_ab_test = active_counts[active_counts['active_ad_count'] < 2]
        for _, row in no_ab_test.iterrows():
            ag = row.get(adgroup_col, 'Unknown')
            camp = row.get(campaign_col, '') if campaign_col else ''
            findings.append({
                "severity": "HIGH",
                "area": "A/B Testing",
                "ad_group": ag,
                "campaign": camp,
                "detail": f"Ad group has only 1 active ad. No split testing in progress.",
                "recommendation": "Add a second RSA with different headline angles to begin testing."
            })

    # ---- Underperforming Ads (vs peer ads in same ad group) ----
    if '_ctr' in df and adgroup_col and campaign_col:
        for (camp, ag), group in active_df.groupby([campaign_col, adgroup_col]):
            if len(group) < 2:
                continue
            group_avg_ctr = group['_ctr'].mean()
            group_avg_cost = group['_cost'].mean() if '_cost' in group else 0

            for idx, row in group.iterrows():
                ctr = row.get('_ctr', 0) or 0
                cost = row.get('_cost', 0) or 0
                conv = row.get('_conv', 0) or 0

                if group_avg_ctr > 0 and ctr < group_avg_ctr * 0.5 and row.get('_impr', 0) > 100:
                    findings.append({
                        "severity": "MEDIUM",
                        "area": "Underperforming Ad",
                        "ad_group": ag,
                        "campaign": camp,
                        "detail": f"Ad CTR {ctr:.2%} is well below ad group average of {group_avg_ctr:.2%}.",
                        "recommendation": "Pause this ad and replace with a new variant testing a different value proposition."
                    })

                if group_avg_cost > 0 and cost > group_avg_cost * 2 and conv == 0:
                    findings.append({
                        "severity": "MEDIUM",
                        "area": "Expensive Non-Converting Ad",
                        "ad_group": ag,
                        "campaign": camp,
                        "detail": f"Ad has spent ${cost:.2f} (2x+ the group average) with 0 conversions.",
                        "recommendation": "Pause this ad. Its messaging is not converting — try a different angle."
                    })

    # ---- URL Consistency ----
    if url_col and campaign_col and adgroup_col:
        for (camp, ag), group in active_df.groupby([campaign_col, adgroup_col]):
            urls = group[url_col].dropna().astype(str)
            domains = urls.apply(lambda u: u.split('/')[2] if '//' in u else u.split('/')[0])
            if domains.nunique() > 1:
                findings.append({
                    "severity": "LOW",
                    "area": "URL Inconsistency",
                    "ad_group": ag,
                    "campaign": camp,
                    "detail": f"Ads in this group point to different domains: {domains.unique().tolist()}. May indicate a configuration error.",
                    "recommendation": "Verify all ads point to the correct domain for this ad group."
                })

    summary = (
        f"Analyzed {len(df)} ads ({len(active_df)} active). "
        f"Ad strength breakdown: {strength_summary}. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "ad_strength_breakdown": strength_summary,
        "metrics": {
            "total_ads": len(df),
            "active_ads": len(active_df),
            "total_cost": round(total_cost, 2),
            "total_conversions": round(total_conv, 1),
        }
    }
