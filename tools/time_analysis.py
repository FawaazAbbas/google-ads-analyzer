"""
Tool: analyze_time_performance
Analyzes hour-of-day and day-of-week performance patterns and recommends ad schedule adjustments.
"""
from .utils import load_csv, clean_currency, clean_percentage, clean_number, safe_divide, find_col


def analyze(tod_path: str = "data/time_of_day.csv", dow_path: str = "data/day_of_week.csv") -> dict:
    findings = []
    tod_results = {}
    dow_results = {}

    # ---- Hour of Day Analysis ----
    try:
        tod = load_csv(tod_path)

        hour_col  = find_col(tod, 'Hour of day', 'Hour')
        cost_col  = find_col(tod, 'Cost', 'Spend')
        conv_col  = find_col(tod, 'Conversions', 'Conv.')
        ctr_col   = find_col(tod, 'CTR')
        click_col = find_col(tod, 'Clicks')

        if hour_col:
            if cost_col:
                tod['_cost'] = tod[cost_col].apply(clean_currency)
            if conv_col:
                tod['_conv'] = tod[conv_col].apply(clean_number)
            if click_col:
                tod['_clicks'] = tod[click_col].apply(clean_number)

            tod['_cpa'] = tod.apply(
                lambda r: safe_divide(r.get('_cost', 0) or 0, r.get('_conv', 0) or 0), axis=1
            )
            tod['_conv_rate'] = tod.apply(
                lambda r: safe_divide(r.get('_conv', 0) or 0, r.get('_clicks', 0) or 0), axis=1
            )

            total_cost = tod['_cost'].sum() if '_cost' in tod else 0
            total_conv = tod['_conv'].sum() if '_conv' in tod else 0
            avg_conv_rate = safe_divide(total_conv, tod['_clicks'].sum() if '_clicks' in tod else 0)

            # Top / worst hours
            tod_sorted = tod.sort_values('_conv_rate', ascending=False)
            top_hours = tod_sorted.head(3)[hour_col].tolist()
            worst_hours = tod_sorted.tail(3)[hour_col].tolist()

            hour_table = []
            for _, row in tod.iterrows():
                hour = row.get(hour_col)
                cost = row.get('_cost', 0) or 0
                conv = row.get('_conv', 0) or 0
                conv_rate = row.get('_conv_rate', 0) or 0
                cpa = row.get('_cpa', 0) or 0

                rec_adj = ((conv_rate / avg_conv_rate) - 1) * 100 if avg_conv_rate > 0 else 0
                rec_adj = max(-90, min(900, rec_adj))

                hour_table.append({
                    "hour": hour,
                    "cost": round(cost, 2),
                    "conversions": round(conv, 1),
                    "conv_rate": f"{conv_rate:.2%}",
                    "cpa": round(cpa, 2),
                    "recommended_adj": f"{rec_adj:+.0f}%",
                })

                # Flag hours with high spend but zero conversions
                cost_share = safe_divide(cost, total_cost)
                if cost_share > 0.05 and conv == 0:
                    findings.append({
                        "severity": "HIGH",
                        "area": "Time of Day — Wasted Spend",
                        "hour": hour,
                        "detail": f"Hour {hour}:00 accounts for {cost_share:.0%} of spend (${cost:.2f}) with 0 conversions.",
                        "recommendation": f"Set a -50% to -100% bid adjustment for hour {hour} in ad schedule settings."
                    })

            tod_results = {
                "hour_table": hour_table,
                "top_performing_hours": top_hours,
                "worst_performing_hours": worst_hours,
            }

    except FileNotFoundError:
        tod_results = {"error": "time_of_day.csv not found — skipping hourly analysis"}

    # ---- Day of Week Analysis ----
    try:
        dow = load_csv(dow_path)

        day_col   = find_col(dow, 'Day of week', 'Day')
        cost_col  = find_col(dow, 'Cost', 'Spend')
        conv_col  = find_col(dow, 'Conversions', 'Conv.')
        click_col = find_col(dow, 'Clicks')

        if day_col:
            if cost_col:
                dow['_cost'] = dow[cost_col].apply(clean_currency)
            if conv_col:
                dow['_conv'] = dow[conv_col].apply(clean_number)
            if click_col:
                dow['_clicks'] = dow[click_col].apply(clean_number)

            dow['_cpa'] = dow.apply(
                lambda r: safe_divide(r.get('_cost', 0) or 0, r.get('_conv', 0) or 0), axis=1
            )
            dow['_conv_rate'] = dow.apply(
                lambda r: safe_divide(r.get('_conv', 0) or 0, r.get('_clicks', 0) or 0), axis=1
            )

            total_cost = dow['_cost'].sum() if '_cost' in dow else 0
            avg_conv_rate = safe_divide(
                dow['_conv'].sum() if '_conv' in dow else 0,
                dow['_clicks'].sum() if '_clicks' in dow else 0
            )

            day_table = []
            for _, row in dow.iterrows():
                day = row.get(day_col)
                cost = row.get('_cost', 0) or 0
                conv = row.get('_conv', 0) or 0
                conv_rate = row.get('_conv_rate', 0) or 0
                cpa = row.get('_cpa', 0) or 0

                rec_adj = ((conv_rate / avg_conv_rate) - 1) * 100 if avg_conv_rate > 0 else 0
                rec_adj = max(-90, min(900, rec_adj))

                day_table.append({
                    "day": day,
                    "cost": round(cost, 2),
                    "conversions": round(conv, 1),
                    "conv_rate": f"{conv_rate:.2%}",
                    "cpa": round(cpa, 2),
                    "recommended_adj": f"{rec_adj:+.0f}%",
                })

                cost_share = safe_divide(cost, total_cost)
                if cost_share > 0.10 and conv == 0:
                    findings.append({
                        "severity": "HIGH",
                        "area": "Day of Week — Wasted Spend",
                        "day": day,
                        "detail": f"{day} accounts for {cost_share:.0%} of spend (${cost:.2f}) with 0 conversions.",
                        "recommendation": f"Set a -50% bid adjustment for {day} or exclude from ad schedule."
                    })

            dow_results = {"day_table": day_table}

    except FileNotFoundError:
        dow_results = {"error": "day_of_week.csv not found — skipping day-of-week analysis"}

    summary = (
        f"Time performance analysis complete. "
        f"Hourly analysis: {'complete' if 'error' not in tod_results else 'skipped'}. "
        f"Day-of-week analysis: {'complete' if 'error' not in dow_results else 'skipped'}. "
        f"Found {len(findings)} issues."
    )

    return {
        "summary": summary,
        "findings": findings,
        "hour_of_day": tod_results,
        "day_of_week": dow_results,
    }
