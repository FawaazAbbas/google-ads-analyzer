"""
Tool: analyze_keywords
Quality Score analysis, match type distribution, expensive non-converting keywords,
duplicate detection, and bid gap analysis.
"""
import pandas as pd
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/keywords.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    name_col      = find_col(df, 'Keyword', 'Keyword text')
    campaign_col  = find_col(df, 'Campaign')
    adgroup_col   = find_col(df, 'Ad group')
    match_col     = find_col(df, 'Match type')
    qs_col        = find_col(df, 'Quality Score', 'Qual. score')
    exp_ctr_col   = find_col(df, 'Exp. CTR', 'Expected CTR')
    ad_rel_col    = find_col(df, 'Ad relevance')
    lp_col        = find_col(df, 'Landing page exp.', 'Landing page experience')
    cost_col      = find_col(df, 'Cost', 'Spend')
    conv_col      = find_col(df, 'Conversions', 'Conv.')
    ctr_col       = find_col(df, 'CTR')
    impr_col      = find_col(df, 'Impressions', 'Impr.')
    cpc_col       = find_col(df, 'Avg. CPC', 'Avg CPC')
    max_cpc_col   = find_col(df, 'Max CPC', 'Max. CPC')
    fp_cpc_col    = find_col(df, 'First page CPC est.', 'First page CPC')
    top_cpc_col   = find_col(df, 'Top of page CPC est.', 'Top of page CPC')
    status_col    = find_col(df, 'Status', 'Keyword status')
    cost_conv_col = find_col(df, 'Cost / conv.', 'Cost/conv.')

    if not name_col:
        return {"error": "Could not find Keyword column in keywords.csv"}

    # Clean columns
    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if ctr_col:
        df['_ctr'] = df[ctr_col].apply(clean_percentage)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)
    if qs_col:
        df['_qs'] = df[qs_col].apply(clean_number)
    if cost_conv_col:
        df['_cpa'] = df[cost_conv_col].apply(clean_currency)
    if max_cpc_col:
        df['_max_cpc'] = df[max_cpc_col].apply(clean_currency)
    if fp_cpc_col:
        df['_fp_cpc'] = df[fp_cpc_col].apply(clean_currency)
    if top_cpc_col:
        df['_top_cpc'] = df[top_cpc_col].apply(clean_currency)
    if cpc_col:
        df['_cpc'] = df[cpc_col].apply(clean_currency)

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_conv = df['_conv'].sum() if '_conv' in df else 0
    avg_cpa = safe_divide(total_cost, total_conv)
    avg_ctr = df['_ctr'].mean() if '_ctr' in df else 0

    # ---- Quality Score Analysis ----
    qs_summary = {}
    if '_qs' in df:
        qs_data = df['_qs'].dropna()
        qs_summary = {
            "poor_1_3": int((qs_data <= 3).sum()),
            "average_4_6": int(((qs_data > 3) & (qs_data <= 6)).sum()),
            "good_7_10": int((qs_data > 6).sum()),
            "avg_quality_score": round(qs_data.mean(), 1),
        }

        # Low QS with meaningful spend
        low_qs = df[df['_qs'] <= 3] if '_qs' in df else pd.DataFrame()
        for _, row in low_qs.iterrows():
            cost = row.get('_cost', 0) or 0
            if cost > 20:
                kw = row.get(name_col, 'Unknown')
                qs = row.get('_qs', 0)
                findings.append({
                    "severity": "HIGH",
                    "area": "Quality Score",
                    "keyword": kw,
                    "campaign": row.get(campaign_col, '') if campaign_col else '',
                    "ad_group": row.get(adgroup_col, '') if adgroup_col else '',
                    "detail": f"Quality Score {qs:.0f}/10 with ${cost:.2f} spend. Low QS inflates your CPC by 2-4x.",
                    "recommendation": "Improve ad relevance or landing page experience. Consider pausing if QS stays below 4."
                })

    # ---- Ad Relevance & Landing Page Issues ----
    if ad_rel_col:
        below_avg_ad = df[df[ad_rel_col].astype(str).str.lower().str.contains('below', na=False)]
        for _, row in below_avg_ad.iterrows():
            cost = row.get('_cost', 0) or 0
            if cost > 10:
                findings.append({
                    "severity": "MEDIUM",
                    "area": "Ad Relevance",
                    "keyword": row.get(name_col, 'Unknown'),
                    "campaign": row.get(campaign_col, '') if campaign_col else '',
                    "ad_group": row.get(adgroup_col, '') if adgroup_col else '',
                    "detail": "Ad relevance is 'Below average'. Your ads don't closely match this keyword.",
                    "recommendation": "Create more specific ads or a dedicated ad group for this keyword theme."
                })

    if lp_col:
        below_avg_lp = df[df[lp_col].astype(str).str.lower().str.contains('below', na=False)]
        for _, row in below_avg_lp.iterrows():
            cost = row.get('_cost', 0) or 0
            if cost > 10:
                findings.append({
                    "severity": "MEDIUM",
                    "area": "Landing Page Experience",
                    "keyword": row.get(name_col, 'Unknown'),
                    "campaign": row.get(campaign_col, '') if campaign_col else '',
                    "ad_group": row.get(adgroup_col, '') if adgroup_col else '',
                    "detail": "Landing page experience is 'Below average'. Google sees this page as irrelevant or slow.",
                    "recommendation": "Align landing page content with keyword intent or improve page speed."
                })

    # ---- Match Type Distribution ----
    match_summary = {}
    if match_col:
        active_df = df
        if status_col:
            active_df = df[~df[status_col].astype(str).str.lower().str.contains('removed|paused', na=False)]

        match_counts = active_df[match_col].value_counts()
        total_kws = match_counts.sum()
        match_summary = match_counts.to_dict()

        broad_count = sum(v for k, v in match_counts.items() if 'broad' in str(k).lower())
        exact_count = sum(v for k, v in match_counts.items() if 'exact' in str(k).lower())

        if total_kws > 0 and (broad_count / total_kws) > 0.60:
            findings.append({
                "severity": "MEDIUM",
                "area": "Match Type Balance",
                "keyword": "All keywords",
                "detail": f"{broad_count}/{total_kws} keywords ({broad_count/total_kws:.0%}) are Broad Match. High risk of irrelevant traffic.",
                "recommendation": "Add more Exact and Phrase match keywords. Set negative keywords to control broad match."
            })

        if exact_count == 0:
            findings.append({
                "severity": "MEDIUM",
                "area": "Match Type Balance",
                "keyword": "All keywords",
                "detail": "No Exact Match keywords found. Missing precision targeting.",
                "recommendation": "Add exact match versions of your top-performing keywords."
            })

    # ---- Expensive Non-Converting Keywords ----
    if '_cost' in df and '_conv' in df:
        waste_threshold = max(avg_cpa * 2, 30) if avg_cpa > 0 else 50
        wasted = df[(df['_cost'] > waste_threshold) & (df['_conv'].fillna(0) == 0)]
        for _, row in wasted.iterrows():
            cost = row.get('_cost', 0) or 0
            kw = row.get(name_col, 'Unknown')
            findings.append({
                "severity": "HIGH",
                "area": "Wasted Spend",
                "keyword": kw,
                "campaign": row.get(campaign_col, '') if campaign_col else '',
                "ad_group": row.get(adgroup_col, '') if adgroup_col else '',
                "detail": f"Spent ${cost:.2f} with 0 conversions.",
                "recommendation": "Pause keyword or reduce max CPC significantly. Add as negative keyword in other campaigns."
            })

    # ---- Duplicate Keywords ----
    if campaign_col and adgroup_col:
        df['_kw_norm'] = df[name_col].astype(str).str.lower().str.strip()
        dupe_counts = df.groupby('_kw_norm')[adgroup_col].nunique()
        dupes = dupe_counts[dupe_counts > 1]
        for kw, count in dupes.items():
            findings.append({
                "severity": "LOW",
                "area": "Duplicate Keywords",
                "keyword": kw,
                "detail": f"Keyword appears in {count} different ad groups. Can cause self-competition and inflated CPCs.",
                "recommendation": "Consolidate to one ad group or use negative keywords to prevent cannibalization."
            })

    # ---- Bid Gaps ----
    if '_max_cpc' in df and '_fp_cpc' in df:
        underbid = df[
            (df['_fp_cpc'].notna()) &
            (df['_max_cpc'].notna()) &
            (df['_fp_cpc'] > df['_max_cpc'] * 1.5)
        ]
        for _, row in underbid.iterrows():
            findings.append({
                "severity": "LOW",
                "area": "Bid Gap",
                "keyword": row.get(name_col, 'Unknown'),
                "detail": f"Max CPC ${row['_max_cpc']:.2f} is well below first-page estimate of ${row['_fp_cpc']:.2f}. Likely not showing on page 1.",
                "recommendation": f"Increase Max CPC to at least ${row['_fp_cpc']:.2f} to compete for page 1 positions."
            })

    summary = (
        f"Analyzed {len(df)} keywords. "
        f"Total spend: ${total_cost:,.2f}. "
        f"Avg Quality Score: {qs_summary.get('avg_quality_score', 'N/A')}. "
        f"Poor QS (1-3): {qs_summary.get('poor_1_3', 0)} keywords. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "quality_score_breakdown": qs_summary,
        "match_type_distribution": match_summary,
        "metrics": {
            "total_keywords": len(df),
            "total_cost": round(total_cost, 2),
            "total_conversions": round(total_conv, 1),
            "avg_cpa": round(avg_cpa, 2),
            "avg_ctr": round(avg_ctr, 4),
        }
    }
