

# bot.py (manager edition, full)
# Discord ↔ GPT Orchestrator ↔ Tradier (live data) / Tradier Sandbox (orders)
# Google Sheets logging (Signals, Trades, Partials, ActiveTrades)
# Features:
# - Natural language trading manager: asks clarifying questions when needed
# - Multi-turn GPT tool-calling with per-channel conversation history
# - Show positions/orders/balances; compute P/L% (avg vs live)
# - Quick intents for immediate stock/option entries (RTH/ETH)
# - Options selection: delta targeting (ATM/ITM/OTM), “next week” expiry
# - Close workflows (options by OCC; equities by symbol) + CONFIRM gate for risky ops
# - Extended-hours stock orders with limit + slippage bps
# - ActiveTrades tracks OCC, strike, expiry; SL→BE updates
# - Positions view that shows ONLY Tradier (as requested)
# - YES/NO routed to manager; "sell all" / percent / qty close for equities
# - Filter so casual chat doesn't hit the signal parser

import os, json, time, math, asyncio, requests, traceback, re
from datetime import datetime, time as dtime
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict, deque

import discord
from discord.ext import commands
from discord import app_commands
from openai import OpenAI

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ---------------- ENV & CONSTANTS ----------------
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

TRADIER_LIVE_API_KEY = os.getenv("TRADIER_LIVE_API_KEY")
TRADIER_SANDBOX_API_KEY = os.getenv("TRADIER_SANDBOX_API_KEY")
TRADIER_SANDBOX_ACCOUNT_ID = os.getenv("TRADIER_SANDBOX_ACCOUNT_ID")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON_TEXT = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_TEXT")

TRADES_TAB        = os.getenv("TRADES_TAB", "Trades")
PARTIALS_TAB      = os.getenv("PARTIALS_TAB", "Partials")
SIGNALS_TAB       = os.getenv("SIGNALS_TAB", "Signals")
ACTIVE_TRADES_TAB = os.getenv("ACTIVE_TRADES_TAB", "ActiveTrades")

TRADIER_LIVE = "https://api.tradier.com"
TRADIER_SANDBOX = "https://sandbox.tradier.com"

MODEL_PRIMARY  = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
MODEL_FALLBACK = os.getenv("OPENAI_MODEL_FALLBACK", "gpt-4.1-mini")

# Extended-hours STOCKS
EXTENDED_STOCK_ENABLE = os.getenv("EXTENDED_STOCK_ENABLE")
if EXTENDED_STOCK_ENABLE is None:
    EXTENDED_STOCK_ENABLE = os.getenv("EXTENDED_STOCK_ENABLED", "false")
EXTENDED_STOCK_ENABLED = EXTENDED_STOCK_ENABLE.lower() in ("1","true","yes")
EXTENDED_LIMIT_SLIPPAGE_BPS = float(os.getenv("EXTENDED_LIMIT_SLIPPAGE_BPS", "10"))  # 10 bps = 0.10%

# Timeframes (seconds)
TF_SECONDS = {"1m":60,"3m":180,"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}
ALLOWED_TFS = set(TF_SECONDS.keys())
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Pending confirmation actions (per-channel)
PENDING_ACTIONS: Dict[str, Dict[str, Any]] = {}

# Per-Discord-channel message history for GPT tool-calling (assistant+user+tool turns)
CHANNEL_HIST = defaultdict(lambda: deque(maxlen=20))  # keep last ~20 turns per channel

# ---------------- TIMEZONE / SESSION ----------------
try:
    import zoneinfo
    NY = zoneinfo.ZoneInfo("America/New_York")
except Exception:
    NY = None

def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def now_ny() -> datetime:
    return datetime.now(tz=NY) if NY else datetime.utcnow()

def market_session_ny(dt: Optional[datetime]=None) -> str:
    """
    Equity sessions: 'pre' (07:00-09:24 ET), 'rth' (09:30-16:00), 'post' (16:00-19:55), else 'closed'.
    Mon–Fri only. If tz not available, return 'rth'.
    """
    dt = dt or now_ny()
    if NY is None:
        return "rth"
    if dt.weekday() >= 5:
        return "closed"
    t = dt.time()
    if dtime(7,0) <= t <= dtime(9,24):
        return "pre"
    if dtime(9,30) <= t <= dtime(16,0):
        return "rth"
    if dtime(16,0) <= t <= dtime(19,55):
        return "post"
    return "closed"

# ---------------- GOOGLE SHEETS ----------------
def sheets_root():
    if not GOOGLE_SERVICE_ACCOUNT_JSON_TEXT:
        raise RuntimeError("Set GOOGLE_SERVICE_ACCOUNT_JSON_TEXT to the raw JSON of your service account.")
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Set GOOGLE_SHEET_ID to your Google Sheet ID.")
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON_TEXT)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def sheets_service():
    return sheets_root().spreadsheets()

def gs_append(tab, values):
    sp = sheets_service()
    sp.values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values":[values]}
    ).execute()

def gs_get_header(tab) -> List[str]:
    sp = sheets_service()
    v = sp.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab}!A1:Z1").execute().get("values", [[]])[0]
    return v

def gs_get_rows(tab, start_row=2, end_col="Z") -> List[List[str]]:
    sp = sheets_service()
    return sp.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab}!A{start_row}:{end_col}").execute().get("values", [])

def gs_find_row(tab, key_col_name, key_val) -> Tuple[Optional[int], List[str]]:
    header = gs_get_header(tab)
    if key_col_name not in header:
        raise RuntimeError(f"Header '{key_col_name}' not found in {tab}")
    col_idx = header.index(key_col_name)
    rows = gs_get_rows(tab)
    for i, row in enumerate(rows, start=2):
        if len(row) > col_idx and row[col_idx] == key_val:
            return i, header
    return None, header

def gs_read_row(tab, rownum, endcol="Z"):
    sp = sheets_service()
    return sp.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab}!A{rownum}:{endcol}{rownum}").execute().get("values", [[]])[0]

def gs_update_row(tab, rownum, values):
    sp = sheets_service()
    sp.values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!A{rownum}",
        valueInputOption="USER_ENTERED",
        body={"values":[values]}
    ).execute()

def gs_delete_row(tab, rownum):
    root = sheets_root()
    meta = root.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheet_id = None
    for sh in meta.get("sheets", []):
        if sh.get("properties", {}).get("title") == tab:
            sheet_id = sh.get("properties", {}).get("sheetId"); break
    if sheet_id is None:
        raise RuntimeError(f"Sheet tab '{tab}' not found to delete row.")
    body = {"requests":[{"deleteDimension":{"range":{"sheetId": sheet_id,"dimension":"ROWS","startIndex": rownum-1,"endIndex": rownum}}}]}
    root.spreadsheets().batchUpdate(spreadsheetId=GOOGLE_SHEET_ID, body=body).execute()

# ---------------- SHEET LOGGING ----------------
def append_trade_history(trade: Dict[str, Any]):
    # Trades columns:
    # trade_id | source | ticker | asset_type | side | contract | qty_total | status | entry_rule | stop_rule | tp_rules
    # | entry_time | entry_price | underlying_at_entry | close_time | close_price | underlying_at_close | realized_pnl_$ | realized_pnl_% | notes
    row = [
        trade.get("trade_id",""), trade.get("source","Discord"), trade.get("ticker",""),
        trade.get("asset_type",""), trade.get("side",""), trade.get("contract",""),
        trade.get("qty_total",""), trade.get("status","waiting_confirm"),
        trade.get("entry_rule",""), trade.get("stop_rule",""), trade.get("tp_rules",""),
        trade.get("entry_time",""), trade.get("entry_price",""),
        trade.get("underlying_at_entry",""), trade.get("close_time",""),
        trade.get("close_price",""), trade.get("underlying_at_close",""),
        trade.get("realized_pnl_$",""), trade.get("realized_pnl_%",""), trade.get("notes","")
    ]
    gs_append(TRADES_TAB, row)

def update_trade_history(trade_id: str, updates: Dict[str, Any]):
    rownum, header = gs_find_row(TRADES_TAB,"trade_id",trade_id)
    if not rownum: return
    cur = gs_read_row(TRADES_TAB, rownum, endcol="Z")
    cur += [""] * (len(header)-len(cur))
    for k,v in updates.items():
        if k in header:
            cur[header.index(k)] = v
    gs_update_row(TRADES_TAB, rownum, cur)

def append_partial(partial: Dict[str,Any]):
    # Partials columns:
    # partial_id | trade_id | type | timestamp | qty | fill_price | underlying_price | target_label | reason | commission_fee | realized_pnl_$ | notes
    row = [
        partial.get("partial_id",""),
        partial.get("trade_id",""),
        partial.get("type",""),
        partial.get("timestamp",""),
        partial.get("qty",""),
        partial.get("fill_price",""),
        partial.get("underlying_price",""),
        partial.get("target_label",""),
        partial.get("reason",""),
        partial.get("commission_fee",""),
        partial.get("realized_pnl_$",""),
        partial.get("notes",""),
    ]
    gs_append(PARTIALS_TAB, row)

def append_signal(sig: Dict[str,Any]):
    # Signals: signal_id | received_at | raw_text | parsed_json
    row = [
        sig.get("signal_id",""),
        sig.get("received_at",""),
        sig.get("raw_text","")[:48000],
        json.dumps(sig.get("parsed_json", {}))[:48000],
    ]
    gs_append(SIGNALS_TAB, row)

def append_active_trade(trade_id: str, parsed: Dict[str,Any], qty: int, status: str="waiting", occ_symbol: str=None, strike: float=None, expiry: str=None):
    # ActiveTrades columns:
    # trade_id | ticker | side | qty | entry_tf | entry_cond | entry_level | stop_tf | stop_cond | stop_level | tps_json | notes | started_at | occ_symbol | strike | expiry
    stop_tf = parsed["stop"]["tf"] if parsed["stop"]["tf"]!="same_as_entry" else parsed["entry"]["tf"]
    row = [
        trade_id,
        parsed.get("ticker",""),
        parsed.get("side",""),
        str(qty),
        parsed["entry"]["tf"],
        parsed["entry"]["cond"],
        str(parsed["entry"]["level"]),
        stop_tf,
        parsed["stop"]["cond"],
        str(parsed["stop"]["level"]),
        json.dumps(parsed.get("tps", [])),
        parsed.get("notes",""),
        now_iso(),
        occ_symbol or "",
        "" if strike is None else str(strike),
        expiry or "",
    ]
    gs_append(ACTIVE_TRADES_TAB, row)

def read_active_trades() -> List[Dict[str,Any]]:
    rows = gs_get_rows(ACTIVE_TRADES_TAB)
    header = gs_get_header(ACTIVE_TRADES_TAB)
    idx = {name:i for i,name in enumerate(header)}
    out=[]
    for r in rows:
        try:
            trade = {
                "trade_id": r[idx.get("trade_id",0)],
                "ticker": r[idx.get("ticker",1)].upper(),
                "side": r[idx.get("side",2)],
                "quantity": int(r[idx.get("qty",3)]),
                "entry": {"tf": r[idx.get("entry_tf",4)], "cond": r[idx.get("entry_cond",5)], "level": float(r[idx.get("entry_level",6)])},
                "stop":  {"tf": r[idx.get("stop_tf",7)],  "cond": r[idx.get("stop_cond",8)],  "level": float(r[idx.get("stop_level",9)])},
                "tps": json.loads(r[idx.get("tps_json",10)] or "[]"),
                "asset_type":"option",
                "notes": r[idx.get("notes",11)] if len(r)>11 else "",
                "occ_symbol": r[idx.get("occ_symbol",13)] if len(r)>13 else "",
                "strike": float(r[idx.get("strike",14)]) if len(r)>14 and r[idx.get("strike",14)] else None,
                "expiry": r[idx.get("expiry",15)] if len(r)>15 else "",
            }
            out.append(trade)
        except Exception:
            continue
    return out

def remove_active_trade(trade_id: str):
    rownum, _ = gs_find_row(ACTIVE_TRADES_TAB, "trade_id", trade_id)
    if rownum:
        gs_delete_row(ACTIVE_TRADES_TAB, rownum)

# ---------------- OPENAI (LLM PARSER) ----------------
client = OpenAI(api_key=OPENAI_API_KEY)

EXTRACT_SYSTEM = """You convert free-form trade alerts into strict JSON for automation.
Rules:
1) Return only JSON. No prose.
2) Normalize timeframes to: 1m,3m,5m,15m,1h,4h,1d.
3) Convert "weeks/days/next Friday" to integer DTE.
4) Option side: calls→long, puts→short.
5) Use conditions: close_above, close_below, touch_above, touch_below.
6) Levels default to underlying price unless "premium" is stated.
7) Take-profits use sell_qty (contracts) or sell_pct; never both in one leg.
8) If delta or strike specified, use it; else choose nearest to default delta 0.50.
9) Default expiry ≈14 DTE if none given.
10) If stop TF not given, use same_as_entry.
Output JSON shape:
{
 "ticker":"", "asset_type":"option|stock",
 "strategy":"single", "side":"long|short", "quantity":1,
 "entry":{"tf":"", "cond":"", "level":0.0},
 "stop":{"tf":"", "cond":"", "level":0.0},
 "tps":[{"trigger":{"cond":"", "level":0.0}, "sell_qty":0}, {"trigger":{"cond":"", "level":0.0}, "sell_pct":0}],
 "option_select":{"dte":14,"delta":0.5,"type":"auto|call|put","strike":null,"choose":"nearest_delta"},
 "session":"RTH","notes":""
}
"""

def parse_alert_to_json(text: str) -> Dict[str,Any]:
    msgs = [{"role":"system","content":EXTRACT_SYSTEM},{"role":"user","content":text}]
    try:
        resp = client.chat.completions.create(model=MODEL_PRIMARY, messages=msgs, temperature=0)
    except Exception:
        resp = client.chat.completions.create(model=MODEL_FALLBACK, messages=msgs, temperature=0)
    raw = resp.choices[0].message.content.strip()
    return json.loads(raw)

# ---------- GPT TOOL SCHEMA & ORCHESTRATOR ----------
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_positions",
            "description": "Return current account positions; optionally filter by symbols",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbols": {"type": "array", "items": {"type": "string"}}
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_orders",
            "description": "Return working/pending orders; optionally filter by symbols",
            "parameters": {
                "type": "object",
                "properties": {
                    "symbols": {"type": "array", "items": {"type": "string"}},
                    "open_only": {"type": "boolean"}
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "close_options",
            "description": "Close specific option positions by OCC symbol list",
            "parameters": {
                "type": "object",
                "properties": {
                    "occ_symbols": {"type":"array","items":{"type":"string"}},
                    "order_type": {"type":"string","enum":["market","limit"]},
                    "limit": {"type":"number"}
                },
                "required": ["occ_symbols"]
            }
        }
    },
    {
        "type":"function",
        "function":{
            "name":"get_positions_with_pl",
            "description":"Return positions with avg, live, and pl_pct; optional symbol filter",
            "parameters":{"type":"object","properties":{"symbols":{"type":"array","items":{"type":"string"}}}}
        }
    },
    {
        "type":"function",
        "function":{
            "name":"prepare_pending_close",
            "description":"Stash a pending close request (occ list) for confirmation",
            "parameters":{
                "type":"object",
                "properties":{
                    "occ_symbols":{"type":"array","items":{"type":"string"}},
                    "order_type":{"type":"string","enum":["market","limit"]},
                    "limit":{"type":"number"},
                    "channel_id":{"type":"string"}
                },
                "required":["occ_symbols","channel_id"]
            }
        }
    },
    # Manager tools
    {
        "type":"function",
        "function":{
            "name":"get_account",
            "description":"Return sandbox account balances and buying power",
            "parameters":{"type":"object","properties":{}}
        }
    },
    {
        "type":"function",
        "function":{
            "name":"close_equities",
            "description":"Close equity positions by symbol list (full/percent/qty)",
            "parameters":{
                "type":"object",
                "properties":{
                    "symbols":{"type":"array","items":{"type":"string"}},
                    "percent":{"type":"integer"},
                    "quantity":{"type":"integer"}
                },
                "required":["symbols"]
            }
        }
    },
    {
        "type":"function",
        "function":{
            "name":"place_equity_order",
            "description":"Place equity market order in sandbox",
            "parameters":{
                "type":"object",
                "properties":{
                    "symbol":{"type":"string"},
                    "side":{"type":"string","enum":["buy","sell"]},
                    "quantity":{"type":"integer","minimum":1}
                },
                "required":["symbol","side","quantity"]
            }
        }
    },
    {
        "type":"function",
        "function":{
            "name":"place_option_order",
            "description":"Place option order by OCC symbol (market)",
            "parameters":{
                "type":"object",
                "properties":{
                    "occ_symbol":{"type":"string"},
                    "side":{"type":"string","enum":["buy_to_open","sell_to_open","buy_to_close","sell_to_close"]},
                    "quantity":{"type":"integer","minimum":1}
                },
                "required":["occ_symbol","side","quantity"]
            }
        }
    }
]

ORCH_SYSTEM = """You are a trading manager for the user’s Tradier SANDBOX account.
Core behavior:
- Understand plain-English trading requests and use tools to fetch positions, orders, balances, quotes, etc.
- If the request is ambiguous or missing key fields (symbol, qty, call/put, expiry, limit/market), ASK A CLEAR QUESTION and WAIT for the user’s answer before trading.
- Prefer read → decide → execute. If size > 5 contracts or notional > $5k, summarize and ask the user to type CONFIRM (use prepare_pending_close for options).
- For P/L or balances, call get_positions_with_pl and/or get_account.
Execution rules:
- Prefer market unless user specifies a limit.
- For “expire this week,” choose the coming Friday. For “next week,” pick next Friday.
- For “in the money / out of the money,” target delta ≈ 0.60 / 0.30 respectively.
Output:
- Keep replies concise. If asking a question, be specific about what you need (e.g., “How many contracts?” “Calls or puts?” “Which expiry?”)."""

def gpt_orchestrate(user_text: str, channel_id: str) -> str:
    # Build message history for this channel
    hist = CHANNEL_HIST[channel_id]

    # Start with system
    messages = [{"role":"system","content":ORCH_SYSTEM}]

    # Replay a compact history (assistant/user/tool messages)
    messages.extend(list(hist))

    # Append this new user turn (include channel_id hint)
    messages.append({"role":"user","content": user_text + f"\n\n[channel_id:{channel_id}]"} )

    max_loops = 6
    for _ in range(max_loops):
        resp = client.chat.completions.create(
            model=MODEL_PRIMARY,
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            temperature=0
        )
        msg = resp.choices[0].message

        # Tool call?
        if getattr(msg, "tool_calls", None):
            # Record assistant tool-call “content” (for continuity)
            messages.append({"role":"assistant","content": msg.content or "", "tool_calls": msg.tool_calls})
            for call in msg.tool_calls:
                name = call.function.name
                args = json.loads(call.function.arguments or "{}")
                if name == "get_positions":
               