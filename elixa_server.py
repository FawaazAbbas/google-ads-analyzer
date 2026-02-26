"""
Google Ads Analyzer — Elixa Webhook Server
-------------------------------------------
Registers as an agent on elixa.app. Elixa POSTs to /invoke when a user
messages the bot; this server fetches live Google Ads data via the Elixa
Tool Gateway (no CSV uploads needed) and returns an AI-powered analysis.

Run locally:
    uvicorn elixa_server:app --host 0.0.0.0 --port 8080

Environment variables:
    ANTHROPIC_API_KEY   — your Anthropic API key
    ELIXA_SECRET        — (optional) shared secret for HMAC-SHA256 auth

Elixa registration:
    Invoke path:  /invoke
    Health path:  /health
    Auth:         API Key  OR  HMAC-SHA256 (set ELIXA_SECRET)
    Manifest:     {"toolsRequired": ["google_ads"], "canMutate": false, "riskTier": "sandbox"}
"""
import hashlib
import hmac
import json
import logging
import os
import time

import httpx
import anthropic
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Google Ads Analyzer (Elixa)")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ELIXA_SECRET = os.environ.get("ELIXA_SECRET", "")  # optional HMAC verification

SYSTEM_PROMPT = (
    "You are a senior Google Ads optimization specialist integrated into the Elixa workspace. "
    "You have access to the user's live Google Ads data pulled directly via their OAuth connection.\n\n"
    "When asked to analyze an account:\n"
    "- Identify the top issues and opportunities — be specific, use real numbers from the data\n"
    "- Group findings by priority: Critical → High → Medium\n"
    "- For each finding: state the problem, its impact, and a concrete action to take\n"
    "- End with a 'Quick Wins' section: 3 actions that take under 30 minutes\n"
    "- Format for chat: use markdown headers and bullet points, keep it readable\n\n"
    "When asked a specific question (e.g. 'how are my keywords doing?'), answer it directly "
    "and concisely, then offer to go deeper.\n\n"
    "If data is missing or incomplete, work with what you have and note the gaps.\n"
    "Never give generic advice — every recommendation must reference the actual data."
)

# ---------------------------------------------------------------------------
# HMAC verification (optional — only runs if ELIXA_SECRET is set)
# ---------------------------------------------------------------------------

def verify_hmac(request_body: bytes, signature: str, timestamp: str) -> bool:
    """
    Verify that the request came from Elixa using HMAC-SHA256.
    Rejects requests older than 5 minutes.
    """
    if not ELIXA_SECRET:
        return True  # auth not configured — skip verification

    try:
        ts = int(timestamp)
        if abs(time.time() - ts) > 300:  # 5-minute replay window
            logger.warning("HMAC timestamp too old: %s", timestamp)
            return False
        payload = "{}.{}".format(timestamp, request_body.decode("utf-8"))
        expected = hmac.new(
            ELIXA_SECRET.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)
    except Exception as e:
        logger.warning("HMAC verification error: %s", e)
        return False

# ---------------------------------------------------------------------------
# Tool Gateway helpers
# ---------------------------------------------------------------------------

async def gateway_call(client: httpx.AsyncClient, gateway_url: str, session_token: str,
                        integration: str, action: str, params: dict = None):
    """
    Call the Elixa Tool Gateway. Returns (data_dict_or_None, error_code_or_None).
    error_code is "missing_connection" | "gateway_error" | None.
    """
    try:
        resp = await client.post(
            gateway_url,
            headers={
                "Authorization": "Bearer {}".format(session_token),
                "Content-Type": "application/json",
            },
            json={
                "integration": integration,
                "action": action,
                "params": params or {},
            },
            timeout=20.0,
        )
        if resp.status_code == 403:
            body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            if body.get("error") == "missing_connection":
                return None, "missing_connection"
            return None, "gateway_error"
        if resp.status_code == 200:
            return resp.json(), None
        return None, "gateway_error"
    except Exception as e:
        logger.warning("Gateway call failed (%s/%s): %s", integration, action, e)
        return None, "gateway_error"


async def fetch_google_ads_data(gateway_url: str, session_token: str) -> dict:
    """
    Fetch all available Google Ads data from the Elixa Tool Gateway.
    Returns a dict with whatever data was successfully retrieved, plus an "errors" list.
    A special key "missing_connection" is set to True if the user hasn't connected Google Ads.
    """
    result = {}
    errors = []

    async with httpx.AsyncClient() as client:

        # --- Campaigns ---
        data, err = await gateway_call(client, gateway_url, session_token,
                                        "google_ads", "get_campaigns")
        if err == "missing_connection":
            return {"missing_connection": True}
        if data is not None:
            result["campaigns"] = data
        elif err:
            errors.append("campaigns")

        # --- Reports: try each dimension ---
        report_dimensions = [
            "keywords", "search_terms", "ads", "ad_groups",
            "devices", "geographic", "audiences",
        ]
        for dimension in report_dimensions:
            data, err = await gateway_call(
                client, gateway_url, session_token,
                "google_ads", "get_reports",
                {"type": dimension, "days": 30},
            )
            if data is not None:
                result[dimension] = data
            # Silently skip dimensions the gateway doesn't support

    if errors:
        result["_fetch_errors"] = errors

    return result

# ---------------------------------------------------------------------------
# Analysis helper
# ---------------------------------------------------------------------------

def run_analysis(user_message: str, ads_data: dict) -> str:
    """Call Claude with the live Google Ads data and return its response text."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Serialise only the data that came back (skip internal error keys)
    clean_data = {k: v for k, v in ads_data.items() if not k.startswith("_")}
    data_json = json.dumps(clean_data, indent=2, default=str)

    # Note any dimensions that failed to load
    missing = ads_data.get("_fetch_errors", [])
    note = ""
    if missing:
        note = "\n\nNote: the following dimensions could not be fetched: {}.".format(
            ", ".join(missing)
        )

    user_content = (
        "User request: {}\n\n"
        "Live Google Ads data (last 30 days, pulled via OAuth):\n"
        "```json\n{}\n```{}"
    ).format(user_message, data_json, note)

    response = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=3000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    return response.content[0].text

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/invoke")
async def invoke(request: Request):
    raw_body = await request.body()

    # --- Optional HMAC verification ---
    if ELIXA_SECRET:
        sig = request.headers.get("X-Elixa-Signature", "")
        ts  = request.headers.get("X-Elixa-Timestamp", "")
        if not verify_hmac(raw_body, sig, ts):
            logger.warning("HMAC verification failed — rejecting request")
            return JSONResponse({"response": "Unauthorized."}, status_code=401)

    # --- Parse body ---
    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        return JSONResponse({"response": "Invalid request — could not parse JSON."}, status_code=400)

    user_message = (body.get("message") or "").strip()
    if not user_message:
        return JSONResponse({"response": "I didn't receive a message. What would you like to know about your Google Ads account?"})

    gateway     = body.get("toolGateway") or {}
    gateway_url = gateway.get("gatewayUrl")
    session_token = gateway.get("sessionToken")

    # --- No gateway credentials → can't fetch data ---
    if not gateway_url or not session_token:
        return JSONResponse({
            "response": (
                "I need access to your Google Ads account to help you. "
                "Please connect Google Ads in **Elixa Settings → Integrations → Google Ads**, "
                "then ask me anything about your account."
            )
        })

    # --- Fetch live Google Ads data ---
    logger.info("Fetching Google Ads data for user %s", body.get("user_id", "unknown"))
    ads_data = await fetch_google_ads_data(gateway_url, session_token)

    if ads_data.get("missing_connection"):
        return JSONResponse({
            "response": (
                "Your Google Ads account isn't connected yet. "
                "Go to **Elixa Settings → Integrations → Google Ads** to connect it, "
                "then I can analyze your campaigns, keywords, spend, and more."
            )
        })

    if not any(k for k in ads_data if not k.startswith("_")):
        return JSONResponse({
            "response": (
                "I wasn't able to fetch any Google Ads data right now — "
                "this might be a temporary issue. Please try again in a moment."
            )
        })

    # --- Run analysis via Claude ---
    logger.info("Running analysis for user %s", body.get("user_id", "unknown"))
    try:
        reply = run_analysis(user_message, ads_data)
    except anthropic.AuthenticationError:
        logger.error("Invalid Anthropic API key")
        return JSONResponse({
            "response": "Configuration error: the Anthropic API key is invalid. Please contact your workspace admin."
        })
    except anthropic.RateLimitError:
        return JSONResponse({
            "response": "I'm hitting API rate limits right now. Please try again in a minute."
        })
    except Exception as e:
        logger.error("Analysis error: %s", e)
        return JSONResponse({
            "response": "I encountered an error analyzing your data. Please try again."
        })

    return JSONResponse({
        "response": reply,
        "tools_used": ["google_ads"],
    })
