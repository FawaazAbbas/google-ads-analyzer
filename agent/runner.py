"""
Generator-based agent loop shared by both the CLI (main.py) and the Streamlit UI (app.py).

Yields (event_type, data) tuples so callers can display live progress without threads.

Event types:
    ("status",     str)   - general status message
    ("thinking",   None)  - Claude is making an API call
    ("tool_start", str)   - tool name about to be executed
    ("tool_done",  str)   - tool name just finished
    ("complete",   str)   - final markdown report text
    ("error",      str)   - fatal error message
"""
import os
from datetime import datetime

import anthropic

from agent.tool_definitions import TOOLS
from agent.tool_executor import execute
from agent.report_writer import save_report


EXPECTED_FILES = [
    "campaigns.csv",
    "ad_groups.csv",
    "keywords.csv",
    "search_terms.csv",
    "ads.csv",
    "extensions.csv",
    "audiences.csv",
    "devices.csv",
    "time_of_day.csv",
    "day_of_week.csv",
    "geographic.csv",
]

FILE_LABELS = {
    "campaigns.csv":    "Campaign Performance",
    "ad_groups.csv":    "Ad Group Structure",
    "keywords.csv":     "Keywords & Quality Score",
    "search_terms.csv": "Search Terms",
    "ads.csv":          "Ad Creatives",
    "extensions.csv":   "Ad Extensions",
    "audiences.csv":    "Audience Segments",
    "devices.csv":      "Device Performance",
    "time_of_day.csv":  "Hour of Day Performance",
    "day_of_week.csv":  "Day of Week Performance",
    "geographic.csv":   "Geographic Performance",
}

SYSTEM_PROMPT = (
    "You are a senior Google Ads optimization specialist with deep expertise in PPC strategy, "
    "Quality Score optimization, bidding strategies, and account structure.\n\n"
    "You have been given access to 12 analysis tools that each examine a different dimension of a Google Ads account "
    "from CSV exports. Your job is to call EVERY available tool, then synthesize ALL findings into a comprehensive "
    "prioritized optimization report.\n\n"
    "ANALYSIS PROTOCOL - follow this order:\n"
    "1. Call analyze_campaign_performance AND analyze_budget_pacing together (account-wide context first)\n"
    "2. Call analyze_keywords AND analyze_search_terms together (highest ROI area)\n"
    "3. Call analyze_ad_creatives AND analyze_ad_group_structure together (creative & structure)\n"
    "4. Call analyze_devices, analyze_time_performance, AND analyze_audiences together (bid adjustments)\n"
    "5. Call analyze_extensions AND analyze_geographic_performance together (coverage & geo)\n"
    "6. Call analyze_bidding_strategies last (strategy alignment)\n\n"
    "After ALL tools have returned:\n"
    "- Write a comprehensive Markdown optimization report\n"
    "- Group findings by priority: CRITICAL -> HIGH -> MEDIUM -> LOW\n"
    "- For every finding, include: the specific problem, affected campaigns/keywords/etc., estimated impact, and a concrete action\n"
    "- Include a Quick Wins section: top 5 actions that take under 30 minutes\n"
    "- Include a 30-Day Action Plan broken into weekly tasks\n"
    "- Be specific -- use actual names, numbers, and percentages from the data\n"
    "- Never give generic advice -- every recommendation must reference the actual data\n\n"
    "If a tool returns an error (missing CSV file), note the skipped analysis and continue with the rest.\n\n"
    "Available data files: {available_files}\n"
    "Report date: {report_date}"
)


def run_analysis(available_files, api_key, data_dir="data"):
    """
    Generator that drives the full agent loop and yields progress events.

    Parameters
    ----------
    available_files : list
        Filenames (e.g. ["campaigns.csv", "keywords.csv"]) present in data_dir.
    api_key : str
        Anthropic API key.
    data_dir : str
        Directory where CSVs live. Defaults to "data".

    Yields
    ------
    (event_type, data) tuples - see module docstring for full list.
    """
    yield ("status", "Initializing...")

    try:
        client = anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        yield ("error", "Failed to create Anthropic client: {}".format(str(e)))
        return

    system = SYSTEM_PROMPT.format(
        available_files=", ".join(available_files),
        report_date=datetime.now().strftime("%B %d, %Y"),
    )

    initial_message = (
        "Please analyze this Google Ads account thoroughly. "
        "Available data files: {}. "
        "Call all relevant tools and produce a comprehensive prioritized optimization report."
    ).format(", ".join(available_files))

    messages = [{"role": "user", "content": initial_message}]

    yield ("status", "Sending {} data files to Claude for analysis...".format(len(available_files)))

    while True:
        yield ("thinking", None)

        try:
            response = client.messages.create(
                model="claude-opus-4-6",
                max_tokens=8096,
                system=system,
                tools=TOOLS,
                messages=messages,
            )
        except anthropic.AuthenticationError:
            yield ("error", "Invalid API key. Please check your Anthropic API key and try again.")
            return
        except anthropic.RateLimitError:
            yield ("error", "Rate limit reached. Please wait a moment and try again.")
            return
        except Exception as e:
            yield ("error", "API call failed: {}".format(str(e)))
            return

        if response.stop_reason == "tool_use":
            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in tool_use_blocks:
                yield ("tool_start", block.name)

                result = execute(block.name, block.input)

                yield ("tool_done", block.name)

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            messages.append({"role": "user", "content": tool_results})

        elif response.stop_reason == "end_turn":
            final_text = next(
                (block.text for block in response.content if hasattr(block, "text")),
                None,
            )

            if not final_text:
                yield ("error", "No final report was generated by the model.")
                return

            try:
                save_report(final_text)
            except Exception:
                pass  # Don't fail the UI just because the file write failed

            yield ("complete", final_text)
            return

        else:
            yield ("error", "Unexpected stop reason from API: {}.".format(response.stop_reason))
            return
