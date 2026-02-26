"""
Tool: analyze_budget_pacing
Checks daily budget utilization, budget-capped campaigns, and projected monthly spend.
"""
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(data_path: str = "data/campaigns.csv", report_days: int = 30) -> dict:
    df = load_csv(data_path)

    findings = []

    name_col    = find_col(df, 'Campaign', 'Campaign name')
    cost_col    = find_col(df, 'Cost', 'Spend')
    budget_col  = find_col(df, 'Budget', 'Daily budget')
    lost_budget = find_col(df, 'Search lost IS (budget)', 'Search Lost IS (budget)')
    status_col  = find_col(df, 'Campaign status', 'Status')
    budget_type = find_col(df, 'Budget type', 'Budget Type')

    if not name_col:
        return {"error": "Could not find Campaign column in campaigns.csv"}

    if cost_col:
        df['_cost'] = df[cost_col].apply(clean_currency)
    if budget_col:
        df['_budget'] = df[budget_col].apply(clean_currency)
    if lost_budget:
        df['_lost_b'] = df[lost_budget].apply(clean_percentage)

    total_cost = df['_cost'].sum() if '_cost' in df else 0
    total_budget_daily = df['_budget'].sum() if '_budget' in df else 0
    projected_monthly = (total_cost / report_days) * 30.4 if report_days > 0 else 0

    pacing_table = []

    for _, row in df.iterrows():
        name = row.get(name_col, 'Unknown')
        cost = row.get('_cost', 0) or 0
        budget = row.get('_budget', 0) or 0
        lost_b = row.get('_lost_b') or 0
        status = str(row.get(status_col, '')).lower() if status_col else ''
        btype = str(row.get(budget_type, '')).lower() if budget_type else ''

        if cost == 0 and status != 'enabled':
            continue

        daily_avg = safe_divide(cost, report_days)
        utilization = safe_divide(daily_avg, budget) if budget > 0 else None
        projected = daily_avg * 30.4

        pacing_table.append({
            "campaign": name,
            "daily_avg_spend": round(daily_avg, 2),
            "daily_budget": round(budget, 2),
            "utilization_pct": f"{utilization:.0%}" if utilization is not None else "N/A",
            "projected_monthly": round(projected, 2),
        })

        # Budget-capped
        if lost_b and lost_b > 0.15:
            findings.append({
                "severity": "HIGH",
                "area": "Budget Cap",
                "campaign": name,
                "detail": f"Losing {lost_b:.0%} of impressions to budget limit. Daily avg spend: ${daily_avg:.2f} vs ${budget:.2f} budget.",
                "recommendation": f"Increase budget to approximately ${daily_avg * 1.3:.2f}/day to capture missed traffic."
            })
        elif utilization is not None and utilization > 0.95 and budget > 0:
            findings.append({
                "severity": "MEDIUM",
                "area": "Budget Utilization",
                "campaign": name,
                "detail": f"Spending {utilization:.0%} of daily budget (${daily_avg:.2f} of ${budget:.2f}). Risk of mid-day budget exhaustion.",
                "recommendation": "Monitor delivery schedule. Consider increasing budget or using shared budgets."
            })

        # Severe under-pacing
        if utilization is not None and utilization < 0.20 and cost > 10:
            findings.append({
                "severity": "LOW",
                "area": "Budget Pacing",
                "campaign": name,
                "detail": f"Only spending {utilization:.0%} of daily budget. Budgeted ${budget:.2f}/day but averaging ${daily_avg:.2f}/day.",
                "recommendation": "Reallocate budget to better-performing campaigns or broaden targeting."
            })

        # Shared budgets (just flag for awareness)
        if 'shared' in btype:
            findings.append({
                "severity": "LOW",
                "area": "Shared Budget",
                "campaign": name,
                "detail": "Uses a shared budget. Shared budgets can cause some campaigns to starve others.",
                "recommendation": "Review shared budget allocation — ensure high-priority campaigns aren't being throttled."
            })

    summary = (
        f"Analyzed budget pacing across {len(df)} campaigns over {report_days} days. "
        f"Total spend: ${total_cost:,.2f}. "
        f"Total daily budget: ${total_budget_daily:,.2f}. "
        f"Projected monthly spend: ${projected_monthly:,.2f}. "
        f"Found {len(findings)} pacing issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "pacing_table": pacing_table,
        "metrics": {
            "total_spend": round(total_cost, 2),
            "total_daily_budget": round(total_budget_daily, 2),
            "projected_monthly_spend": round(projected_monthly, 2),
            "report_days": report_days,
        }
    }
