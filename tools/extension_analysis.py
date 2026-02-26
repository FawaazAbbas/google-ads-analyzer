"""
Tool: analyze_extensions
Audits ad extension type coverage and performance. Flags missing types and underperformers.
"""
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


EXPECTED_EXTENSIONS = ['Sitelink', 'Callout', 'Call', 'Structured snippet', 'Image', 'Lead form']
HIGH_IMPACT_EXTENSIONS = ['Sitelink', 'Callout']
SITELINK_MIN = 4


def analyze(data_path: str = "data/extensions.csv") -> dict:
    df = load_csv(data_path)
    findings = []

    type_col     = find_col(df, 'Extension type', 'Type')
    campaign_col = find_col(df, 'Campaign')
    adgroup_col  = find_col(df, 'Ad group')
    status_col   = find_col(df, 'Status')
    cost_col     = find_col(df, 'Cost', 'Spend')
    conv_col     = find_col(df, 'Conversions', 'Conv.')
    ctr_col      = find_col(df, 'CTR')
    impr_col     = find_col(df, 'Impressions', 'Impr.')
    clicks_col   = find_col(df, 'Clicks')

    if not type_col:
        return {"error": "Could not find 'Extension type' column in extensions.csv"}

    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if conv_col:
        df['_conv'] = df[conv_col].apply(clean_number)
    if ctr_col:
        df['_ctr'] = df[ctr_col].apply(clean_percentage)
    if impr_col:
        df['_impr'] = df[impr_col].apply(clean_number)

    active_df = df
    if status_col:
        active_df = df[df[status_col].astype(str).str.lower().str.contains('enabled|active', na=False)]

    account_avg_ctr = df['_ctr'].mean() if '_ctr' in df else 0

    # --- Coverage Check ---
    present_types = set(active_df[type_col].astype(str).str.strip().unique())
    missing_types = []

    for ext_type in EXPECTED_EXTENSIONS:
        found = any(ext_type.lower() in t.lower() for t in present_types)
        if not found:
            severity = "HIGH" if ext_type in HIGH_IMPACT_EXTENSIONS else "MEDIUM"
            missing_types.append(ext_type)
            findings.append({
                "severity": severity,
                "area": "Missing Extension",
                "extension_type": ext_type,
                "detail": f"No active '{ext_type}' extensions found.",
                "recommendation": f"Add {ext_type} extensions — {'critical for CTR improvement' if ext_type in HIGH_IMPACT_EXTENSIONS else 'improves ad real estate and CTR'}."
            })

    # --- Sitelink Count Check ---
    sitelink_rows = active_df[active_df[type_col].astype(str).str.lower().str.contains('sitelink', na=False)]
    if len(sitelink_rows) < SITELINK_MIN:
        findings.append({
            "severity": "HIGH",
            "area": "Sitelink Count",
            "extension_type": "Sitelink",
            "detail": f"Only {len(sitelink_rows)} active sitelinks found. Google recommends a minimum of {SITELINK_MIN}.",
            "recommendation": f"Add {SITELINK_MIN - len(sitelink_rows)} more sitelinks. Include links to key pages like Contact, Pricing, About, and specific services."
        })

    # --- Paused Extensions ---
    if status_col:
        paused = df[df[status_col].astype(str).str.lower().str.contains('paused', na=False)]
        if len(paused) > 0:
            paused_types = paused[type_col].value_counts().to_dict()
            findings.append({
                "severity": "LOW",
                "area": "Paused Extensions",
                "extension_type": "Various",
                "detail": f"{len(paused)} paused extensions found: {paused_types}.",
                "recommendation": "Review paused extensions and re-enable or remove them to improve ad coverage."
            })

    # --- Underperforming Extensions ---
    if '_ctr' in df and account_avg_ctr > 0:
        for _, row in active_df.iterrows():
            ctr = row.get('_ctr', 0) or 0
            impr = row.get('_impr', 0) or 0
            ext_type = row.get(type_col, 'Unknown')

            if impr > 500 and ctr < account_avg_ctr * 0.3:
                findings.append({
                    "severity": "LOW",
                    "area": "Underperforming Extension",
                    "extension_type": ext_type,
                    "detail": f"{ext_type} CTR {ctr:.2%} is well below account avg {account_avg_ctr:.2%}.",
                    "recommendation": "Rewrite this extension with more compelling, benefit-focused copy."
                })

    # --- Campaign-level coverage ---
    if campaign_col:
        campaigns = df[campaign_col].dropna().unique() if campaign_col else []
        no_ext_campaigns = []
        for camp in campaigns:
            camp_exts = active_df[active_df[campaign_col] == camp]
            if len(camp_exts) == 0:
                no_ext_campaigns.append(camp)
                findings.append({
                    "severity": "HIGH",
                    "area": "Campaign Extension Coverage",
                    "campaign": camp,
                    "detail": "Campaign has no active extensions.",
                    "recommendation": "Add at minimum Sitelinks and Callouts to this campaign."
                })

    summary = (
        f"Analyzed {len(df)} extension entries. "
        f"Active extension types: {list(present_types)}. "
        f"Missing high-impact extensions: {missing_types}. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "present_extension_types": list(present_types),
        "missing_extension_types": missing_types,
        "metrics": {
            "total_extensions": len(df),
            "active_extensions": len(active_df),
            "sitelink_count": len(sitelink_rows),
        }
    }
