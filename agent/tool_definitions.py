"""
All 12 tool schemas in Anthropic API format.
This list is passed directly to client.messages.create(tools=TOOLS).
"""

TOOLS = [
    {
        "name": "analyze_campaign_performance",
        "description": (
            "Analyzes campaign-level performance including budget utilization, impression share, "
            "ROAS, CPA, and CTR. Identifies campaigns that are budget-limited, have poor impression "
            "share, or are underperforming against account averages. Returns findings with severity levels."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to campaigns.csv",
                    "default": "data/campaigns.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_budget_pacing",
        "description": (
            "Checks daily budget utilization across campaigns, identifies campaigns limited by budget, "
            "shared budget conflicts, and projects monthly spend. Flags over-pacing and under-pacing issues."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to campaigns.csv",
                    "default": "data/campaigns.csv"
                },
                "report_days": {
                    "type": "integer",
                    "description": "Number of days in the report date range",
                    "default": 30
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_keywords",
        "description": (
            "Deep keyword analysis: Quality Score distribution (poor/average/good), match type balance, "
            "expensive non-converting keywords, duplicate keywords across ad groups, ad relevance issues, "
            "landing page experience issues, and bid gaps vs first-page estimates."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to keywords.csv",
                    "default": "data/keywords.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_search_terms",
        "description": (
            "Analyzes search term reports to find: (1) high-performing search terms to harvest as exact match "
            "keywords, (2) irrelevant search terms draining budget that should become negative keywords, "
            "(3) common themes for negative keyword lists. Quantifies wasted spend."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to search_terms.csv",
                    "default": "data/search_terms.csv"
                },
                "keywords_path": {
                    "type": "string",
                    "description": "Path to keywords.csv for cross-referencing",
                    "default": "data/keywords.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_ad_creatives",
        "description": (
            "Evaluates ad creative quality: ad strength ratings (Poor/Average/Good/Excellent), "
            "A/B test coverage (flags ad groups with only 1 active ad), underperforming ads vs "
            "their ad group peers, and URL consistency across ads in the same ad group."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to ads.csv",
                    "default": "data/ads.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_ad_group_structure",
        "description": (
            "Analyzes ad group structural health: keyword count per ad group (flags groups with 20+ keywords), "
            "single-keyword ad groups (SKAGs), ad groups with high spend and zero conversions, "
            "and campaigns with an excessive number of ad groups."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to ad_groups.csv",
                    "default": "data/ad_groups.csv"
                },
                "keywords_path": {
                    "type": "string",
                    "description": "Path to keywords.csv",
                    "default": "data/keywords.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_bidding_strategies",
        "description": (
            "Reviews bidding strategy suitability based on conversion volume (Smart Bidding needs 30+ conv/month), "
            "identifies campaigns under-investing despite good performance, flags negative ROAS situations, "
            "and highlights CPA misalignment across campaigns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to campaigns.csv",
                    "default": "data/campaigns.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_audiences",
        "description": (
            "Analyzes audience segment performance including remarketing lists, in-market audiences, "
            "and demographic segments. Identifies missing remarketing setup, audiences needing positive "
            "bid adjustments (high performers), and audiences wasting budget (should be excluded or reduced)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to audiences.csv",
                    "default": "data/audiences.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_devices",
        "description": (
            "Compares Mobile, Desktop, and Tablet performance (CPA, conv rate, CTR, cost share). "
            "Calculates recommended bid adjustment percentages for each device based on relative "
            "conversion rate vs account average. Flags mobile UX issues and missed device opportunities."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to devices.csv",
                    "default": "data/devices.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_time_performance",
        "description": (
            "Analyzes hour-of-day and day-of-week performance patterns. Identifies peak conversion windows "
            "and off-hours wasting budget. Produces specific bid adjustment recommendations for ad scheduling "
            "with percentage adjustments per hour/day."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "tod_path": {
                    "type": "string",
                    "description": "Path to time_of_day.csv",
                    "default": "data/time_of_day.csv"
                },
                "dow_path": {
                    "type": "string",
                    "description": "Path to day_of_week.csv",
                    "default": "data/day_of_week.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_extensions",
        "description": (
            "Audits ad extension coverage: checks which extension types are missing (Sitelinks, Callouts, "
            "Call, Structured snippets etc.), verifies minimum sitelink count (4+), identifies paused extensions, "
            "flags underperforming extensions, and checks which campaigns have no extensions at all."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to extensions.csv",
                    "default": "data/extensions.csv"
                }
            },
            "required": []
        }
    },
    {
        "name": "analyze_geographic_performance",
        "description": (
            "Analyzes geographic performance at country, region, and city level. Identifies high-value "
            "locations to increase bids on, locations wasting budget to exclude, and quantifies potential "
            "savings from geographic exclusions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "Path to geographic.csv",
                    "default": "data/geographic.csv"
                }
            },
            "required": []
        }
    },
]
