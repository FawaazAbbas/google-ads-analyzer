"""
Dispatches tool calls from Claude to the appropriate analysis function.
Handles missing CSV files gracefully so Claude can continue with available data.
"""
import json
import sys
import os

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import (
    campaign_performance,
    budget_pacing,
    keyword_analysis,
    search_term_analysis,
    ad_creative_analysis,
    ad_group_structure,
    bidding_strategy,
    audience_analysis,
    device_analysis,
    time_analysis,
    extension_analysis,
    geo_analysis,
)

TOOL_MAP = {
    "analyze_campaign_performance":   campaign_performance.analyze,
    "analyze_budget_pacing":          budget_pacing.analyze,
    "analyze_keywords":               keyword_analysis.analyze,
    "analyze_search_terms":           search_term_analysis.analyze,
    "analyze_ad_creatives":           ad_creative_analysis.analyze,
    "analyze_ad_group_structure":     ad_group_structure.analyze,
    "analyze_bidding_strategies":     bidding_strategy.analyze,
    "analyze_audiences":              audience_analysis.analyze,
    "analyze_devices":                device_analysis.analyze,
    "analyze_time_performance":       time_analysis.analyze,
    "analyze_extensions":             extension_analysis.analyze,
    "analyze_geographic_performance": geo_analysis.analyze,
}


def execute(tool_name: str, tool_input: dict) -> str:
    """
    Execute a tool by name with the given input dict.
    Returns a JSON string — Anthropic tool results must be strings or content blocks.
    Never raises — returns an error dict on failure so Claude can handle it gracefully.
    """
    func = TOOL_MAP.get(tool_name)
    if func is None:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})

    try:
        result = func(**tool_input)
        return json.dumps(result, indent=2, default=str)
    except FileNotFoundError as e:
        return json.dumps({
            "error": f"CSV file not found: {e}. This analysis will be skipped.",
            "skipped": True
        })
    except Exception as e:
        return json.dumps({
            "error": f"Tool '{tool_name}' failed: {str(e)}. Skipping this analysis.",
            "skipped": True
        })
