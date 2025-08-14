
# bot.py (manager edition)
# Discord ↔ GPT Orchestrator (multi-turn, clarifying Qs) ↔ Tradier (live data) / Tradier Sandbox (orders)
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
                    data = sandbox_list_positions_filtered(args.get("symbols"))
                elif name == "get_orders":
                    data = sandbox_list_orders_filtered(args.get("symbols"), open_only=args.get("open_only", True))
                elif name == "close_options":
                    data = close_options_occ(args["occ_symbols"], args.get("order_type","market"), args.get("limit"))
                elif name == "get_positions_with_pl":
                    data = sandbox_list_positions_detailed()
                    syms = args.get("symbols")
                    if syms:
                        syms_set = set(s.upper() for s in syms)
                        data = [d for d in data if d["underlying"] in syms_set]
                elif name == "prepare_pending_close":
                    occs = args["occ_symbols"]; order_type = args.get("order_type","market"); limit_px = args.get("limit")
                    ch = str(args.get("channel_id", channel_id))
                    PENDING_ACTIONS[ch] = {"ts": time.time(),"occ_symbols": occs,"order_type": order_type,"limit": limit_px}
                    data = {"ok": True, "count": len(occs)}
                elif name == "get_account":
                    data = sandbox_get_account_balances()
                elif name == "close_equities":
                    data = close_equities_by_symbol(args.get("symbols",[]), pct=args.get("percent"), qty_abs=args.get("quantity"))
                elif name == "place_equity_order":
                    data = place_equity_market(args["symbol"], args["side"], int(args["quantity"]))
                elif name == "place_option_order":
                    data = place_option_market_by_occ(args["occ_symbol"], args["side"], int(args["quantity"]))
                else:
                    data = {"ok": False, "error": "unknown_tool"}

                # Append tool result into the running conversation
                messages.append({
                    "role":"tool",
                    "tool_call_id": call.id,
                    "name": name,
                    "content": json.dumps(data)
                })
            # continue loop so GPT can read tool results and decide next step
            continue

        # No tool call: we have a normal assistant message (could be a clarifying question or final answer)
        final_text = msg.content or "Done."

        # Persist this exchange to the channel history (compact)
        hist.append({"role":"user","content": user_text})
        hist.append({"role":"assistant","content": final_text})

        # Trim history automatically (deque maxlen handles it)
        CHANNEL_HIST[channel_id] = hist

        return final_text

    # Fallback
    hist.append({"role":"user","content": user_text})
    hist.append({"role":"assistant","content": "Timed out while orchestrating."})
    return "Timed out while orchestrating."

# ---------------- SANITIZATION ----------------
def normalize_tf(tf: str) -> str:
    tf = (tf or "").strip().lower()
    return tf if tf in ALLOWED_TFS else "15m"

def sanitize_parsed(p: Dict[str,Any]) -> Tuple[Optional[Dict[str,Any]], List[str]]:
    warnings = []
    if not isinstance(p, dict):
        return None, ["Parser did not return JSON."]
    t = (p.get("ticker") or "").strip().upper()
    if not t:
        return None, ["No ticker detected."]
    p["ticker"] = t
    ent = p.get("entry",{}) or {}
    ent["tf"] = normalize_tf(ent.get("tf"))
    if ent.get("cond") not in {"close_above","close_below","touch_above","touch_below"}:
        ent["cond"] = "close_above" if p.get("side","long")=="long" else "close_below"
        warnings.append("Unsupported entry cond; default applied.")
    try:
        ent["level"] = float(ent.get("level"))
    except Exception:
        return None, ["Invalid entry level."]
    p["entry"] = ent
    st = p.get("stop",{}) or {}
    st_tf = st.get("tf")
    st["tf"] = ent["tf"] if (not st_tf or st_tf=="same_as_entry") else normalize_tf(st_tf)
    if st.get("cond") not in {"close_above","close_below","touch_above","touch_below"}:
        st["cond"] = "close_below" if p.get("side","long")=="long" else "close_above"
        warnings.append("Unsupported stop cond; default applied.")
    try:
        st["level"] = float(st.get("level"))
    except Exception:
        return None, ["Invalid stop level."]
    p["stop"] = st
    try:
        p["quantity"] = int(p.get("quantity",1))
    except Exception:
        p["quantity"] = 1
        warnings.append("Invalid quantity; defaulted to 1.")
    good_tps=[]
    for tp in p.get("tps",[]):
        trig = tp.get("trigger",{})
        cond = trig.get("cond"); lvl = trig.get("level")
        if cond not in {"touch_above","touch_below","close_above","close_below"}: continue
        try: lvl = float(lvl)
        except Exception: continue
        leg = {"trigger":{"cond":cond,"level":lvl}}
        if "sell_qty" in tp: leg["sell_qty"] = int(tp["sell_qty"])
        elif "sell_pct" in tp: leg["sell_pct"] = int(tp["sell_pct"])
        else: leg["sell_qty"] = 1
        good_tps.append(leg)
    p["tps"]=good_tps
    return p, warnings

# ---------------- TRADIER HELPERS ----------------
def live_quote(symbol: str) -> Optional[float]:
    if not symbol:
        return None
    h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
    r = requests.get(f"{TRADIER_LIVE}/v1/markets/quotes", params={"symbols":symbol}, headers=h, timeout=10)
    r.raise_for_status()
    q = r.json().get("quotes",{}).get("quote",{})
    if not q: return None
    if isinstance(q, list) and q:
        q = q[0]
    try:
        bid = q.get("bid"); ask = q.get("ask")
        if bid is not None and ask is not None and float(ask) > 0:
            return (float(bid)+float(ask))/2.0
        return float(q.get("last") or q.get("bid") or 0)
    except Exception:
        return None

def _choose_expiration_by_dte(exps: List[str], desired_dte: int, target_hint: Optional[str]=None) -> Optional[str]:
    # exps: list of "YYYY-MM-DD"
    today = datetime.utcnow().date()
    if target_hint == "next_week":
        candidates = []
        for e in exps:
            try:
                d = datetime.strptime(e, "%Y-%m-%d").date()
            except Exception:
                continue
            dte = (d - today).days
            if 6 <= dte <= 13 and d.weekday() == 4:  # Friday
                candidates.append((abs(dte-7), e))
        if candidates:
            candidates.sort()
            return candidates[0][1]
    scored = []
    for e in exps:
        try:
            d = datetime.strptime(e, "%Y-%m-%d").date()
        except Exception:
            continue
        dte = (d - today).days
        if dte < 0: 
            continue
        scored.append((abs(dte - desired_dte), dte, e))
    if not scored:
        return exps[0] if exps else None
    scored.sort()
    return scored[0][2]

def get_option_chain(symbol: str, dte: int=14, greeks=True, target_hint: Optional[str]=None):
    h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
    exp_resp = requests.get(f"{TRADIER_LIVE}/v1/markets/options/expirations",
                            params={"symbol":symbol,"includeAllRoots":"true"},
                            headers=h, timeout=10).json()
    exps = exp_resp.get("expirations",{}).get("date",[])
    chosen = _choose_expiration_by_dte(exps, desired_dte=dte, target_hint=target_hint)
    chain = requests.get(f"{TRADIER_LIVE}/v1/markets/options/chains",
                         params={"symbol":symbol,"expiration":chosen,"greeks":"true" if greeks else "false"},
                         headers=h, timeout=10).json()
    return chain.get("options",{}).get("option",[]), chosen

def pick_contract(chain: List[Dict[str,Any]], side: str, target_delta: float=0.5, strike: Optional[float]=None, typ: str="auto"):
    if typ=="auto":
        typ = "call" if side=="long" else "put"
    candidates = [c for c in chain if c.get("option_type")==typ]
    if not candidates:
        raise RuntimeError("No matching option_type candidates in chain.")
    if strike is not None:
        return min(candidates, key=lambda c: abs(float(c["strike"])-float(strike)))
    return min(candidates, key=lambda c: abs(abs(float(c.get("delta",0))) - float(target_delta)))

def sandbox_place_option_order(occ_symbol: str, action: str, qty: int, order_type="market", limit_price=None, duration="day"):
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    data = {"class":"option","symbol": occ_symbol,"side": action,"quantity": qty,"type": order_type,"duration": duration}
    if limit_price is not None:
        data["price"] = f"{float(limit_price):.2f}"
    url = f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/orders"
    r = requests.post(url, data=data, headers=h, timeout=15)
    # Robust error surfacing
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"Tradier order HTTP {r.status_code}: {r.text[:500]}") from e
    try:
        return r.json()
    except Exception:
        return {"status_code": r.status_code, "raw": r.text[:500]}

def sandbox_place_equity_order(symbol: str, side: str, qty: int, last_price: float):
    """
    Stocks: RTH → MARKET/DAY; pre/post → LIMIT with duration=pre/post (if EXTENDED_STOCK_ENABLED).
    """
    sess = market_session_ny()
    if sess == "rth" or not EXTENDED_STOCK_ENABLED:
        order_type = "market"; duration = "day"; price = None
    elif sess in ("pre","post"):
        order_type = "limit"; duration = "pre" if sess == "pre" else "post"
        bump = (EXTENDED_LIMIT_SLIPPAGE_BPS / 10000.0) * last_price
        price = (last_price + bump) if side.lower().startswith("buy") else (last_price - bump)
        price = round(price, 2)
    else:
        raise RuntimeError("Market closed (no pre/post). Try during pre, RTH, or post.")
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    data = {"class":"equity","symbol":symbol,"side":side,"quantity":qty,"type":order_type,"duration":duration}
    if price is not None: data["price"] = f"{price:.2f}"
    url = f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/orders"
    r = requests.post(url, data=data, headers=h, timeout=15)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"Tradier equity order HTTP {r.status_code}: {r.text[:500]}") from e
    try:
        return r.json()
    except Exception:
        return {"status_code": r.status_code, "raw": r.text[:500]}

def sandbox_list_positions():
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    url = f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/positions"
    r = requests.get(url, headers=h, timeout=15)
    r.raise_for_status()
    pos = r.json().get("positions",{}).get("position",[])
    if isinstance(pos, dict): pos = [pos]
    return pos or []

def sandbox_list_orders(include_all=False):
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    url = f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/orders"
    r = requests.get(url, headers=h, timeout=15)
    r.raise_for_status()
    data = r.json().get("orders", {}).get("order", [])
    if isinstance(data, dict):
        data = [data]
    if include_all:
        return data
    final_states = {"filled","canceled","cancelled","expired","rejected","done","closed"}
    openish = [o for o in data if str(o.get("status","")).lower() not in final_states]
    return openish

# ---------- TOOL ADAPTERS ----------
def sandbox_list_positions_filtered(symbols=None):
    pos = sandbox_list_positions()
    if symbols:
        syms = set(s.upper() for s in symbols)
        pos = [p for p in pos if (p.get("underlying") or p.get("symbol","")).upper() in syms]
    out=[]
    for p in pos:
        cls = (p.get("class") or "").lower()
        qty = int(abs(int(p.get("quantity",0) or 0)))
        sym = p.get("symbol")
        ul  = p.get("underlying") or p.get("symbol")
        exp = p.get("expiry") or infer_expiry_from_occ(sym) if cls=="option" else None
        out.append({"class": cls,"symbol": sym,"underlying": (ul or "").upper(),"quantity": qty,"expiry": exp})
    return out

def infer_expiry_from_occ(occ_symbol: Optional[str]) -> Optional[str]:
    if not occ_symbol: return None
    m = re.search(r"(\d{6})", occ_symbol)
    if not m: return None
    yymmdd = m.group(1)
    yy, mm, dd = yymmdd[:2], yymmdd[2:4], yymmdd[4:6]
    yyyy = int(yy) + 2000
    return f"{yyyy:04d}-{int(mm):02d}-{int(dd):02d}"

def sandbox_list_orders_filtered(symbols=None, open_only=True):
    orders = sandbox_list_orders(include_all=not open_only)
    if symbols:
        syms = set(s.upper() for s in symbols)
        orders = [o for o in orders if (o.get("underlying") or o.get("underlying_symbol") or o.get("symbol","")).upper() in syms]
    out=[]
    for o in orders:
        out.append({
            "id": o.get("id"),
            "class": (o.get("class") or "").lower(),
            "symbol": o.get("symbol"),
            "underlying": (o.get("underlying") or o.get("underlying_symbol") or o.get("symbol","")).upper(),
            "side": o.get("side"),
            "type": (o.get("type") or "").lower(),
            "quantity": o.get("quantity"),
            "status": (o.get("status") or "").lower(),
            "duration": (o.get("duration") or "day").lower(),
            "price": o.get("price")
        })
    return out

def close_options_occ(occ_symbols, order_type="market", limit=None):
    pos = sandbox_list_positions()
    qty_by_occ = {}
    for p in pos:
        if p.get("class")!="option": continue
        occ = p.get("symbol")
        if occ in occ_symbols:
            qty_by_occ[occ] = int(abs(int(p.get("quantity",0) or 0)))
    res = {"closed":0,"details":[]}
    for occ in occ_symbols:
        q = qty_by_occ.get(occ, 0)
        if q <= 0:
            res["details"].append({"occ":occ,"result":"no_position"}); continue
        if order_type=="limit" and limit is not None:
            r = sandbox_place_option_order(occ, action="sell_to_close", qty=q, order_type="limit", limit_price=float(limit))
        else:
            r = sandbox_place_option_order(occ, action="sell_to_close", qty=q, order_type="market")
        res["closed"] += q
        res["details"].append({"occ":occ,"qty":q,"result":r})
    return res

def sandbox_list_positions_detailed():
    """Return positions with avg cost (if provided), live price, and PL%."""
    pos = sandbox_list_positions()
    if not pos: return []
    eq_syms = []; occ_syms = []
    for p in pos:
        if p.get("class") == "equity":
            eq_syms.append(p.get("symbol"))
        elif p.get("class") == "option":
            occ_syms.append(p.get("symbol"))
    quotes_map = {}
    def fetch_quotes(symbols):
        if not symbols: return {}
        h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
        r = requests.get(f"{TRADIER_LIVE}/v1/markets/quotes", params={"symbols": ",".join(symbols)}, headers=h, timeout=12)
        r.raise_for_status()
        q = r.json().get("quotes",{}).get("quote",[])
        if isinstance(q, dict): q=[q]
        out={}
        for row in q:
            sym = row.get("symbol")
            bid = row.get("bid"); ask = row.get("ask"); last = row.get("last")
            if bid is not None and ask is not None and float(ask) > 0:
                px = (float(bid)+float(ask))/2.0
            elif last is not None:
                px = float(last)
            else:
                px = float(bid or 0)
            out[sym] = px
        return out
    quotes_map.update(fetch_quotes(eq_syms))
    quotes_map.update(fetch_quotes(occ_syms))
    detailed=[]
    for p in pos:
        cls = (p.get("class") or "").lower()
        qty = int(abs(int(p.get("quantity",0) or 0)))
        sym = p.get("symbol")
        ul  = (p.get("underlying") or p.get("symbol") or "").upper()
        avg = None
        if p.get("price") not in (None, "", "null"):
            try: avg = float(p.get("price"))
            except: avg = None
        if avg is None and p.get("cost_basis") not in (None, "", "null"):
            try:
                cb = float(p.get("cost_basis")); avg = cb / qty if qty else None
            except: pass
        live = quotes_map.get(sym) or quotes_map.get(ul) or None
        pl_pct = None
        if avg is not None and live is not None and avg != 0:
            pl_pct = 100.0 * (live - avg) / avg
        detailed.append({
            "class": cls,"symbol": sym,"underlying": ul,"quantity": qty,
            "avg_price": avg,"live_price": live,"pl_pct": pl_pct,
            "expiry": infer_expiry_from_occ(sym) if cls=="option" else None
        })
    return detailed

# ---- Account / balances ----
def sandbox_get_account_balances():
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    r = requests.get(f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/balances", headers=h, timeout=12)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        return {"error": f"HTTP {r.status_code}", "raw": r.text[:500]}
    try:
        data = r.json().get("balances", {})
    except Exception:
        return {"error":"non_json", "raw": r.text[:500]}
    return {
        "cash": data.get("cash",{}).get("cash_available", data.get("cash",{}).get("cash")),
        "equity": data.get("total_equity"),
        "buying_power": data.get("margin",{}).get("day_trading_buying_power") or data.get("cash",{}).get("cash_available"),
        "maintenance_requirement": data.get("margin",{}).get("maintenance_requirement"),
        "raw": data
    }

# ---- Equities close by symbol ----
def close_equities_by_symbol(symbols: List[str], pct: Optional[int]=None, qty_abs: Optional[int]=None):
    symset = set(s.upper() for s in symbols)
    pos = sandbox_list_positions()
    res = {"closed":0,"details":[]}
    remaining_abs = qty_abs or 0
    for p in pos:
        if p.get("class")!="equity": continue
        sym = (p.get("symbol") or p.get("underlying") or "").upper()
        if sym not in symset: continue
        qty = int(abs(int(p.get("quantity",0) or 0)))
        if qty <= 0: continue
        if pct is not None:
            q_close = max(1, (qty * pct)//100)
        elif qty_abs is not None:
            q_close = min(qty, remaining_abs); remaining_abs -= q_close
        else:
            q_close = qty
        if q_close <= 0: continue
        last = live_quote(sym) or 0.0
        r = sandbox_place_equity_order(sym, side="sell", qty=q_close, last_price=last)
        res["closed"] += q_close
        res["details"].append({"symbol":sym, "qty":q_close, "resp":r})
        if qty_abs is not None and remaining_abs <= 0:
            break
    return res

# ---- Generic place helpers ----
def place_equity_market(symbol: str, side: str, qty: int):
    last = live_quote(symbol) or 0.0
    return sandbox_place_equity_order(symbol=symbol.upper(), side=side, qty=qty, last_price=last)

def place_option_market_by_occ(occ: str, side: str, qty: int):
    return sandbox_place_option_order(occ_symbol=occ, action=side, qty=qty, order_type="market")

# ---------------- STATE (minimal, used for buttons) ----------------
class PositionStore:
    def __init__(self): self.state: Dict[str,Dict[str,Any]] = {}
    def add(self, tid:str, rec:Dict[str,Any]): self.state[tid]=rec
    def get(self, tid:str): return self.state.get(tid)
    def all_active(self): return {k:v for k,v in self.state.items() if v.get("status") in ("active","tp_partial")}
    def set(self, tid, key, val):
        if tid in self.state: self.state[tid][key]=val

positions = PositionStore()

# ---------------- DISCORD BOT ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

class ActionView(discord.ui.View):
    def __init__(self, trade_id:str):
        super().__init__(timeout=None); self.trade_id=trade_id
        self.add_item(CloseAllButton(trade_id))
        self.add_item(Close25Button(trade_id))
        self.add_item(MoveSLBEButton(trade_id))

class CloseAllButton(discord.ui.Button):
    def __init__(self, tid): super().__init__(label="Close All", style=discord.ButtonStyle.danger); self.tid=tid
    async def callback(self, itx: discord.Interaction):
        await itx.response.send_message(f"Confirm close all `{self.tid}` → `!closeall {self.tid} --confirm`", ephemeral=True)

class Close25Button(discord.ui.Button):
    def __init__(self, tid): super().__init__(label="Close 25%", style=discord.ButtonStyle.secondary); self.tid=tid
    async def callback(self, itx: discord.Interaction):
        await itx.response.send_message(f"Confirm close 25% `{self.tid}` → `!close25 {self.tid} --confirm`", ephemeral=True)

class MoveSLBEButton(discord.ui.Button):
    def __init__(self, tid): super().__init__(label="SL → BE", style=discord.ButtonStyle.primary); self.tid=tid
    async def callback(self, itx: discord.Interaction):
        await itx.response.send_message(f"Confirm SL→BE `{self.tid}` → `!moveSLBE {self.tid} --confirm`", ephemeral=True)

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    try:
        synced = await bot.tree.sync()
        print(f"Slash commands synced: {len(synced)}")
    except Exception as e:
        print("Slash sync error:", e)
    try:
        active = read_active_trades()
        for rec in active:
            tid = rec["trade_id"]
            asyncio.create_task(safe_watch(tid, rec, None))
            positions.add(tid, {
                "contract": f"{rec['ticker']} (active)",
                "remaining_qty": rec.get("quantity", 0),
                "avg_entry_premium": 0.0,
                "underlying_at_entry": 0.0,
                "stop_desc": f"{rec['stop']['tf']} {rec['stop']['cond']} {rec['stop']['level']}",
                "tp_desc": ", ".join([str(tp['trigger']['level']) for tp in rec.get('tps',[])]),
                "status": "active",
            })
        print(f"Reloaded {len(active)} active trade(s) from {ACTIVE_TRADES_TAB}.")
    except Exception as e:
        print("ActiveTrades reload error:", e)

# Slash: /positions (Tradier-only)
@bot.tree.command(name="positions", description="Show positions & working orders from Tradier (sandbox)")
async def slash_positions(interaction: discord.Interaction):
    await show_positions(interaction)

@bot.tree.command(name="health", description="Check Sheets + Tradier connectivity")
async def health(interaction: discord.Interaction):
    try:
        sp = sheets_service()
        _ = sp.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{TRADES_TAB}!A1:A1").execute()
        h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
        r = requests.get(f"{TRADIER_LIVE}/v1/markets/quotes", params={"symbols":"SPY"}, headers=h, timeout=10)
        r.raise_for_status()
        await interaction.response.send_message("✅ Health OK: Sheets + Tradier reachable.", ephemeral=True)
    except Exception as e:
        err = "".join(traceback.format_exception_only(type(e), e)).strip()
        await interaction.response.send_message(f"⚠️ Health FAIL: {err}", ephemeral=True)

@bot.command(name="positions")
async def positions_cmd(ctx):
    await show_positions(ctx)

@bot.command(name="reset")
async def reset_cmd(ctx):
    CHANNEL_HIST[str(ctx.channel.id)].clear()
    await ctx.send("🧹 Conversation history for this channel has been cleared.")

async def show_positions(ctx_or_inter):
    """Shows ONLY Tradier sandbox positions + pending/working orders."""
    try:
        pos = sandbox_list_positions()
    except Exception as e:
        pos = []; print("Positions fetch error:", e)

    try:
        open_orders = sandbox_list_orders(include_all=False)
    except Exception as e:
        open_orders = []; print("Orders fetch error:", e)

    lines = []
    if pos:
        lines.append("**📦 Open Positions (Tradier Sandbox)**")
        for p in pos:
            asset_cls = p.get("class","").lower()
            qty = abs(int(p.get("quantity",0) or 0))
            if qty <= 0:
                continue
            if asset_cls == "equity":
                sym = p.get("symbol") or p.get("underlying") or "?"
                lines.append(f"• {sym} — {qty} shares")
            elif asset_cls == "option":
                occ = p.get("symbol","?"); ul = p.get("underlying","?")
                lines.append(f"• {occ} (UL: {ul}) — {qty} contract(s)")
            else:
                lines.append(f"• {p.get('symbol','?')} — {qty} (class: {asset_cls})")
    else:
        lines.append("**📦 Open Positions (Tradier Sandbox)**\n• None")

    if open_orders:
        lines.append("\n**📝 Working / Pending Orders (Tradier Sandbox)**")
        for o in open_orders:
            cls = o.get("class","").lower()
            status = o.get("status","?").upper()
            side = o.get("side","?")
            typ = o.get("type","?").upper()
            qty = o.get("quantity","?")
            dur = o.get("duration","day").upper()
            limit_px = o.get("price")
            sym = o.get("symbol","?")
            ul = o.get("underlying") or o.get("underlying_symbol") or ""
            tail = f" @ {limit_px}" if limit_px is not None else ""
            if cls == "option" and ul:
                lines.append(f"• [{status}] {side} {qty} {sym} ({ul}) — {typ}{tail} / {dur}")
            else:
                lines.append(f"• [{status}] {side} {qty} {sym} — {typ}{tail} / {dur}")
    else:
        lines.append("\n**📝 Working / Pending Orders (Tradier Sandbox)**\n• None")

    content = "\n".join(lines)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(content, ephemeral=True)
    else:
        await ctx_or_inter.send(content)

# ---------------- BUTTON COMMANDS ----------------
@bot.command()
async def closeall(ctx, trade_id: str, confirm: str = ""):
    if confirm != "--confirm": return await ctx.send("Add `--confirm` to proceed.")
    rec = positions.get(trade_id)
    if not rec: return await ctx.send("Trade not found.")
    qty = int(rec["remaining_qty"])
    if qty <= 0: return await ctx.send("Nothing to close.")
    append_partial({
        "partial_id": f"{trade_id}-{int(time.time())}",
        "trade_id": trade_id, "type":"manual_close","timestamp": now_iso(),
        "qty": -qty, "fill_price":"", "underlying_price":"",
        "target_label":"Close All", "reason":"manual", "commission_fee":"",
        "realized_pnl_$":"", "notes":""
    })
    positions.set(trade_id,"remaining_qty",0)
    update_trade_history(trade_id, {"status":"closed","close_time":now_iso()})
    remove_active_trade(trade_id)
    await ctx.send(f"📕 **Trade Closed** — qty {qty} — `{trade_id}`")

@bot.command()
async def close25(ctx, trade_id: str, confirm: str = ""):
    if confirm != "--confirm": return await ctx.send("Add `--confirm` to proceed.")
    rec = positions.get(trade_id)
    if not rec: return await ctx.send("Trade not found.")
    qty = int(rec["remaining_qty"])
    slice_qty = 1 if qty <= 1 else max(1, qty//4)
    append_partial({
        "partial_id": f"{trade_id}-{int(time.time())}",
        "trade_id": trade_id, "type":"tp","timestamp": now_iso(),
        "qty": -slice_qty, "fill_price":"", "underlying_price":"",
        "target_label":"Close 25%", "reason":"manual", "commission_fee":"",
        "realized_pnl_$":"", "notes":""
    })
    positions.set(trade_id,"remaining_qty",qty - slice_qty)
    update_trade_history(trade_id, {"status":"tp_partial"})
    await ctx.send(f"🎯 **Closed 25%** — {slice_qty} — Remaining {qty - slice_qty} — `{trade_id}`")

@bot.command()
async def moveSLBE(ctx, trade_id: str, confirm: str = ""):
    if confirm != "--confirm": return await ctx.send("Add `--confirm` to proceed.")
    rec = positions.get(trade_id)
    if not rec: return await ctx.send("Trade not found.")
    be_desc = f"BE @ underlying {rec.get('underlying_at_entry',0):.2f}"
    positions.set(trade_id,"stop_desc", be_desc)
    update_trade_history(trade_id, {"stop_rule": be_desc, "notes":"SL→BE"})
    await ctx.send(f"✏️ **SL moved to BE** — {be_desc} — `{trade_id}`")

# ---------------- QUICK-INTENT PARSERS ----------------
def try_immediate_stock_intent(text: str):
    m = re.search(r"\b(buy|sell)\s+(?:(\d+)\s+)?([A-Za-z]{1,5})\s+shares\b", text, re.I)
    if not m: return None
    side = "buy" if m.group(1).lower()=="buy" else "sell"
    qty = int(m.group(2) or 1)
    ticker = m.group(3).upper()
    eth = bool(re.search(r"\b(eth|extended|pre[- ]?market|post[- ]?market|after[- ]?hours)\b", text, re.I))
    return {"ticker":ticker,"side":side,"qty":qty,"session":"ETH" if eth else "RTH"}

def try_immediate_option_intent(text: str):
    m = re.search(r"\b(?:(\d+)\s*)?([A-Za-z]{1,5})\s+(call|calls|put|puts)\b", text, re.I)
    if not m: return None
    qty = int(m.group(1) or 1)
    ticker = m.group(2).upper()
    opt_type = m.group(3).lower()
    side = "long" if "call" in opt_type else "short"
    eth = bool(re.search(r"\b(eth|extended|pre[- ]?market|post[- ]?market|after[- ]?hours)\b", text, re.I))

    # Defaults
    dte = 14
    delta = 0.5

    # “in the money” / “out of the money”
    if re.search(r"\bin the money\b|\bitm\b", text, re.I):
        delta = 0.60
    if re.search(r"\bout of the money\b|\botm\b", text, re.I):
        delta = 0.30

    # “next week”
    target_hint = None
    if re.search(r"\bnext\s+week\b", text, re.I):
        dte = 7
        target_hint = "next_week"

    return {
        "ticker": ticker,
        "asset_type": "option",
        "side": side,
        "quantity": qty,
        "session": "ETH" if eth else "RTH",
        "option_select": {"dte": dte, "delta": delta, "type": "auto", "choose":"nearest_delta", "target_hint": target_hint}
    }

def try_close_option_intent(text: str):
    m = re.search(r"\bclose\s+(?:(\d+)%\s+|(\d+)\s+)?([A-Za-z]{1,5})\s+(options|calls?|puts?)\b(?:.*?\bat\s+limit\s+(\d+(?:\.\d+)?))?", text, re.I)
    if not m:
        m2 = re.search(r"\bclose\s+([A-Za-z]{1,5})\s+(options|calls?|puts?)\b(?:.*?\bat\s+limit\s+(\d+(?:\.\d+)?))?", text, re.I)
        if not m2: return None
        pct = None; qty_abs = None
        ticker = m2.group(1).upper(); kind = m2.group(2).lower()
        limit_px = float(m2.group(3)) if m2.group(3) else None
        filt = None if "option" in kind else ("call" if "call" in kind else "put")
        return {"ticker":ticker,"type_filter":filt,"pct":pct,"qty_abs":qty_abs,"limit":limit_px}
    pct = int(m.group(1)) if m.group(1) else None
    qty_abs = int(m.group(2)) if m.group(2) else None
    ticker = m.group(3).upper(); kind = m.group(4).lower()
    limit_px = float(m.group(5)) if m.group(5) else None
    filt = None if "option" in kind else ("call" if "call" in kind else "put")
    return {"ticker":ticker,"type_filter":filt,"pct":pct,"qty_abs":qty_abs,"limit":limit_px}

def try_move_sl_be_intent(text: str):
    t = text.lower()
    if "break even" not in t and "breakeven" not in t and "sl→be" not in t and "to be" not in t:
        return None
    m_tkr = re.search(r"\b([A-Za-z]{1,5})\b", text)
    m_sp = re.search(r"\b(\d+(?:\.\d+)?)\s*([cp])\b", text, re.I)
    m_exp = re.search(r"\b(\d{1,2})/(\d{1,2})\b", text) or re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if not (m_tkr and m_sp and m_exp): return None
    ticker = m_tkr.group(1).upper(); strike = float(m_sp.group(1)); tchar = m_sp.group(2).lower()
    opt_type = "call" if tchar == "c" else "put"
    if len(m_exp.groups())==2:
        mm = int(m_exp.group(1)); dd = int(m_exp.group(2)); yr = datetime.utcnow().year
        expiry = f"{yr:04d}-{mm:02d}-{dd:02d}"
    else:
        expiry = f"{int(m_exp.group(1)):04d}-{int(m_exp.group(2)):02d}-{int(m_exp.group(3)):02d}"
    return {"action":"move_sl_be","ticker":ticker,"type":opt_type,"strike":strike,"expiry":expiry}

def find_active_trades_by_contract(ticker: str, opt_type: str, strike: float, expiry: str):
    acts = read_active_trades(); matches=[]
    for a in acts:
        if a.get("ticker") != ticker.upper(): continue
        occ = (a.get("occ_symbol") or "").upper()
        if occ:
            if (opt_type=="call" and not occ.endswith("C")) or (opt_type=="put" and not occ.endswith("P")): continue
        if a.get("strike") is not None and a.get("expiry"):
            if abs(float(a["strike"])-float(strike))<0.01 and str(a["expiry"])==expiry:
                matches.append(a)
    return matches

def move_stop_to_break_even(trade_id: str):
    rownum, header = gs_find_row(TRADES_TAB, "trade_id", trade_id)
    if not rownum:
        return False, "trade_id not found in Trades"
    cur = gs_read_row(TRADES_TAB, rownum, endcol="Z")
    try: idx_entry_under = header.index("underlying_at_entry")
    except ValueError: return False, "Trades missing 'underlying_at_entry' column"
    try: be_level = float(cur[idx_entry_under])
    except Exception: be_level = None
    be_desc = f"BE @ underlying {be_level:.2f}" if be_level is not None else "BE @ entry underlying"
    update_trade_history(trade_id, {"stop_rule": be_desc, "notes":"SL→BE"})
    positions.set(trade_id, "stop_desc", be_desc)
    rownumA, headerA = gs_find_row(ACTIVE_TRADES_TAB, "trade_id", trade_id)
    if rownumA:
        curA = gs_read_row(ACTIVE_TRADES_TAB, rownumA, endcol="Z"); curA += [""]*(len(headerA)-len(curA))
        try:
            ent_tf_idx = headerA.index("entry_tf"); stop_tf_idx = headerA.index("stop_tf")
            stop_cond_idx = headerA.index("stop_cond"); stop_level_idx = headerA.index("stop_level")
        except ValueError:
            return True, "ActiveTrades missing stop columns; updated Trades/in-memory only"
        if not curA[stop_tf_idx]: curA[stop_tf_idx] = curA[ent_tf_idx]
        curA[stop_cond_idx] = "close_below"
        if be_level is not None: curA[stop_level_idx] = f"{be_level:.2f}"
        gs_update_row(ACTIVE_TRADES_TAB, rownumA, curA)
    return True, be_desc

# ---------------- MESSAGE LISTENER ----------------
async def safe_watch(trade_id, parsed, channel: Optional[discord.TextChannel]):
    try:
        await watch_and_execute(trade_id, parsed, channel)
    except Exception as e:
        if channel: await channel.send(f"⚠️ Watcher stopped for `{trade_id}`: {e}")
        else: print(f"Watcher stopped for {trade_id}: {e}")

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot: return
    content = message.content.strip()
    if not content: return
    await bot.process_commands(message)
    if content.startswith("!"): return

    # CONFIRM flow: execute pending close for this channel
    if content.strip().upper() == "CONFIRM":
        key = str(message.channel.id)
        pending = PENDING_ACTIONS.get(key)
        if not pending:
            return await message.channel.send("Nothing pending to confirm.")
        res = close_options_occ(pending["occ_symbols"], pending.get("order_type","market"), pending.get("limit"))
        del PENDING_ACTIONS[key]
        lines = [f"✅ Executed close for {len(res['details'])} leg(s):"]
        for d in res["details"]:
            if d.get("result") == "no_position":
                lines.append(f"• {d['occ']}: no open qty")
            else:
                lines.append(f"• {d['occ']}: closed {d.get('qty','?')} @ {pending.get('order_type','market')}{(' ' + str(pending.get('limit'))) if pending.get('limit') else ''}")
        return await message.channel.send("\n".join(lines))

    # Orchestrator router for "manager" requests (multi-turn GPT tools + clarifying Qs)
    lowered = content.lower()
    orchestrate_triggers = (
        lowered.startswith(("show ","list ","get ","close ","buy ","sell ","what ","how ")) or
        "balance" in lowered or "profit" in lowered or "p/l" in lowered or "buying power" in lowered
    )
    if orchestrate_triggers:
        try:
            result_text = gpt_orchestrate(content, channel_id=str(message.channel.id))
            return await message.channel.send(result_text)
        except Exception as e:
            print("Orchestrator error:", e)

    # Move SL → BE quick intent
    msl = try_move_sl_be_intent(content)
    if msl:
        matches = find_active_trades_by_contract(msl["ticker"], msl["type"], msl["strike"], msl["expiry"])
        if not matches:
            return await message.channel.send("ℹ️ No matching active option trade found for that contract. (Ensure the bot opened/logged it.)")
        count=0; notes=[]
        for a in matches:
            ok, desc = move_stop_to_break_even(a["trade_id"])
            if ok:
                count+=1; notes.append(f"• `{a['trade_id']}` → {desc}")
        if count==0:
            return await message.channel.send("⚠️ Could not update SL to BE. Check sheet headers/columns.")
        return await message.channel.send("✏️ **SL moved to Break-Even**\n" + "\n".join(notes))

    # Quick intents: CLOSE OPTIONS
    ci = try_close_option_intent(content)
    if ci:
        tkr = ci["ticker"]; filt = ci["type_filter"]; pct = ci["pct"]; qty_abs = ci["qty_abs"]; limit_px = ci["limit"]
        ul_last = live_quote(tkr) or 0.0
        res = close_options_by_underlying(tkr, filt, pct, qty_abs, limit_px)
        if res["reason"] == "no_matching_positions":
            return await message.channel.send(f"ℹ️ No open {tkr} option positions to close.")
        closed = res["closed"]; flavor = "options" if filt is None else (filt + "s")
        limit_txt = f" at limit {limit_px:.2f}" if limit_px is not None else " at market"
        qty_txt = (f"{pct}% of" if pct is not None else (f"{qty_abs} of" if qty_abs is not None else "all"))
        await message.channel.send(f"✅ Closed **{qty_txt} {tkr} {flavor}**{limit_txt} — Underlying last {ul_last:.2f}\nTotal contracts closed: **{closed}**")
        for d in res["details"]:
            await message.channel.send(f"• Sold to close {d['qty']} × {d['occ']}{' @ limit' if limit_px is not None else ' @ market'}")
        return

    # Quick intents: IMMEDIATE STOCK
    isty = try_immediate_stock_intent(content)
    if isty:
        tkr = isty["ticker"]; qty = int(isty["qty"]); side = isty["side"]; session = isty["session"]
        last = live_quote(tkr) or 0.0
        global EXTENDED_STOCK_ENABLED
        prev = EXTENDED_STOCK_ENABLED
        if session == "ETH": EXTENDED_STOCK_ENABLED = True
        try:
            resp = sandbox_place_equity_order(tkr, side, qty, float(last))
        finally:
            EXTENDED_STOCK_ENABLED = prev
        trade_id = f"{tkr}-{datetime.utcnow().strftime('%Y%m%d')}-{int(time.time())%100000:05d}"
        append_trade_history({
            "trade_id": trade_id, "source":"Discord", "ticker":tkr,
            "asset_type":"stock", "side": ("long" if side=="buy" else "short"),
            "contract": f"{tkr} shares","qty_total": qty, "status":"active", "entry_time": now_iso(),
            "underlying_at_entry": f"{last:.2f}", "notes": f"Immediate stock entry ({session})"
        })
        append_active_trade(trade_id, {
            "ticker": tkr, "side": ("long" if side=="buy" else "short"),
            "entry":{"tf":"1m","cond":"touch_above","level": last},
            "stop":{"tf":"1m","cond":"close_below","level": 0}, "tps": [], "notes": f"Immediate {session}"
        }, qty=qty, status="active")
        positions.add(trade_id, {"contract": f"{tkr} shares","remaining_qty": qty,"avg_entry_premium": 0.0,
                                 "underlying_at_entry": last,"stop_desc": "—","tp_desc": "—","status": "active"})
        return await message.channel.send(f"✅ **Stock order sent** — {qty}× {tkr} ({session})\nTrade ID: `{trade_id}` | Underlying last: {last:.2f}")

    # Quick intents: IMMEDIATE OPTION
    iopt = try_immediate_option_intent(content)
    if iopt:
        ticker = iopt["ticker"]; qty = int(iopt["quantity"]); side = iopt["side"]; session = iopt["session"]
        last = live_quote(ticker) or 0.0
        selconf = iopt["option_select"]
        chain, expiry = get_option_chain(ticker, dte=selconf.get("dte",14), target_hint=selconf.get("target_hint"))
        sel = pick_contract(chain=chain, side=side, target_delta=float(selconf.get("delta",0.5)), strike=None, typ=selconf.get("type","auto"))
        occ = sel["symbol"]
        contract_desc = f"{ticker} {expiry} {sel['strike']}{sel['option_type'][0].upper()} (Δ {float(sel.get('delta',0)):.2f})"
        resp = sandbox_place_option_order(occ_symbol=occ, action="buy_to_open", qty=qty, order_type="market", duration="day")
        if isinstance(resp, dict) and "raw" in resp:
            return await message.channel.send(f"⚠️ Tradier order response wasn’t JSON:\n```{resp['raw']}```")
        if isinstance(resp, dict) and resp.get("errors"):
            return await message.channel.send(f"⚠️ Tradier order error:\n```{json.dumps(resp, indent=2)[:900]}```")
        trade_id = f"{ticker}-{datetime.utcnow().strftime('%Y%m%d')}-{int(time.time())%100000:05d}"
        append_trade_history({
            "trade_id": trade_id, "source":"Discord", "ticker":ticker,
            "asset_type":"option", "side": side, "contract": contract_desc,
            "qty_total": qty, "status":"active", "entry_time": now_iso(),
            "underlying_at_entry": f"{last:.2f}", "notes": f"Immediate option entry ({session})"
        })
        append_active_trade(trade_id, {"ticker": ticker, "side": side, "entry":{"tf":"1m","cond":"touch_above","level": last},
                                       "stop":{"tf":"1m","cond":"close_below","level": 0}, "tps": [], "notes": f"Immediate {session}"},
                            qty=qty, status="active", occ_symbol=occ, strike=float(sel['strike']), expiry=str(expiry))
        positions.add(trade_id, {"contract": contract_desc,"remaining_qty": qty,"avg_entry_premium": 0.0,
                                 "underlying_at_entry": last,"stop_desc": "—","tp_desc": "—","status": "active"})
        await message.channel.send(f"✅ **Option order sent** — {qty}× {contract_desc} @ Market ({session})\nTrade ID: `{trade_id}` | Underlying last: {last:.2f}")
        return

    # Rule-based signals (LLM parser)
    try:
        raw = parse_alert_to_json(content)
    except Exception as e:
        return await message.channel.send(f"⚠️ Parse error: {e}")
    parsed, warns = sanitize_parsed(raw)
    if not parsed:
        return await message.channel.send("⚠️ Signal ignored: missing/invalid trade details (ticker/levels).")
    sig_id = f"sig-{int(time.time())}"
    append_signal({"signal_id":sig_id,"received_at":now_iso(),"raw_text":content,"parsed_json":parsed})
    if warns: await message.channel.send("ℹ️ " + " | ".join(warns))
    ticker = parsed["ticker"]
    entry_rule = f"{parsed['entry']['tf']} {parsed['entry']['cond']} {parsed['entry']['level']}"
    stop_rule  = f"{parsed['stop']['tf']} {parsed['stop']['cond']} {parsed['stop']['level']}"
    tp_rules   = ", ".join([f"{('qty '+str(tp.get('sell_qty')) if 'sell_qty' in tp else ('pct '+str(tp.get('sell_pct'))))} @ {tp['trigger']['level']}" for tp in parsed.get('tps',[])])
    trade_id = f"{ticker}-{datetime.utcnow().strftime('%Y%m%d')}-{int(time.time())%100000:05d}"
    append_trade_history({"trade_id": trade_id, "source":"Discord", "ticker":ticker,"asset_type": parsed.get("asset_type","option"),
                          "side": parsed.get("side","long"),"contract": "", "qty_total": parsed.get("quantity",1),
                          "status":"waiting_confirm","entry_rule": entry_rule, "stop_rule": stop_rule,
                          "tp_rules": tp_rules, "notes": parsed.get("notes","")})
    append_active_trade(trade_id, parsed, qty=int(parsed.get("quantity",1)), status="waiting")
    await message.channel.send(f"📥 Signal queued `{trade_id}` for {ticker}. Watching for {entry_rule}.")
    asyncio.create_task(safe_watch(trade_id, parsed, message.channel))

# ---------------- WATCHERS (rule-based) ----------------
def _tf_seconds(tf: str) -> int:
    return TF_SECONDS.get(tf, TF_SECONDS["15m"])

async def watch_and_execute(trade_id: str, parsed: Dict[str,Any], channel: Optional[discord.TextChannel]):
    ticker = parsed["ticker"].upper(); tf = parsed["entry"]["tf"]; tf_sec = _tf_seconds(tf)
    level = float(parsed["entry"]["level"]); cond = parsed["entry"]["cond"]
    if not ticker:
        if channel: await channel.send("⚠️ Aborting: empty ticker.")
        remove_active_trade(trade_id); return
    cur_bucket=None; o=h=l=c=None
    bucket = lambda ts: int(ts//tf_sec)*tf_sec
    while True:
        try:
            last = live_quote(ticker)
        except Exception as e:
            if channel: await channel.send(f"⚠️ Quote error for {ticker}: {e}")
            await asyncio.sleep(2); continue
        if last is None:
            await asyncio.sleep(1); continue
        now_ts = time.time(); b = bucket(now_ts)
        if cur_bucket != b:
            if cur_bucket is not None and c is not None:
                trigger = (cond=="close_above" and c > level) or (cond=="close_below" and c < level)
                if trigger:
                    await place_entry_and_manage(trade_id, parsed, channel); return
            cur_bucket=b; o=h=l=c=last
        else:
            h=max(h,last); l=min(l,last); c=last
        await asyncio.sleep(1)

async def place_entry_and_manage(trade_id:str, parsed:Dict[str,Any], channel: Optional[discord.TextChannel]):
    ticker = parsed["ticker"].upper(); qty = int(parsed.get("quantity",1))
    side = parsed.get("side","long"); asset_type = parsed.get("asset_type","option")
    contract_desc = ""; underlying_now = live_quote(ticker) or 0.0
    if asset_type == "stock":
        contract_desc = f"{ticker} shares"; eq_side = "buy" if side == "long" else "sell_short"
        _ = sandbox_place_equity_order(ticker, eq_side, qty, float(underlying_now))
    else:
        selconf = parsed.get("option_select",{}) or {}
        chain, expiry = get_option_chain(ticker, dte=selconf.get("dte",14), target_hint=selconf.get("target_hint"))
        sel = pick_contract(chain=chain, side=side, target_delta=float(selconf.get("delta",0.5)),
                            strike=selconf.get("strike"), typ=selconf.get("type","auto"))
        occ = sel["symbol"]
        contract_desc = f"{ticker} {expiry} {sel['strike']}{sel['option_type'][0].upper()} (Δ {float(sel.get('delta',0)):.2f})"
        _ = sandbox_place_option_order(occ_symbol=occ, action="buy_to_open", qty=qty, order_type="market")
    positions.add(trade_id,{"contract": contract_desc,"remaining_qty": qty,"avg_entry_premium": 0.0,
                            "underlying_at_entry": underlying_now,"stop_desc": f"{parsed['stop']['tf']} {parsed['stop']['cond']} {parsed['stop']['level']}",
                            "tp_desc": ", ".join([str(tp['trigger']['level']) for tp in parsed.get('tps',[])]),"status":"active"})
    update_trade_history(trade_id, {"status":"active","entry_time": now_iso(),"contract": contract_desc, "underlying_at_entry": f"{underlying_now:.2f}"})
    if channel: await channel.send(f"✅ **Trade Opened** — {contract_desc}\nQty: {qty} | Trade ID: `{trade_id}`", view=ActionView(trade_id))
    asyncio.create_task(tp_sl_manager(trade_id, parsed, channel))

async def tp_sl_manager(trade_id:str, parsed:Dict[str,Any], channel: Optional[discord.TextChannel]):
    ticker = parsed["ticker"].upper(); tf = parsed["entry"]["tf"]; tf_sec = _tf_seconds(tf)
    stop_level = float(parsed["stop"]["level"]); stop_cond = parsed["stop"]["cond"]
    cur_bucket=None; o=h=l=c=None; bucket = lambda ts: int(ts//tf_sec)*tf_sec
    tps = parsed.get("tps",[])
    while True:
        last = live_quote(ticker)
        if last is None: await asyncio.sleep(1); continue
        rec = positions.get(trade_id)
        if not rec: return
        remaining = int(rec["remaining_qty"])
        for tp in list(tps):
            lvl = float(tp["trigger"]["level"]); cond = tp["trigger"]["cond"]
            touch = (cond.endswith("above") and last >= lvl) or (cond.endswith("below") and last <= lvl)
            if touch and remaining>0:
                if "sell_qty" in tp: slice_qty = min(int(tp["sell_qty"]), remaining)
                elif "sell_pct" in tp: slice_qty = max(1, math.floor(remaining * (int(tp["sell_pct"])/100)))
                else: slice_qty = 1
                remaining -= slice_qty; positions.set(trade_id,"remaining_qty",remaining)
                append_partial({"partial_id": f"{trade_id}-{int(time.time())}","trade_id": trade_id, "type":"tp","timestamp": now_iso(),
                                "qty": -slice_qty, "fill_price":"", "underlying_price": last,"target_label": f"TP @ {lvl}",
                                "reason":"touch","commission_fee":"", "realized_pnl_$":"", "notes":""})
                if channel: await channel.send(f"🎯 **TP Hit** — {slice_qty} closed @ underlying {last:.2f} — Remaining {remaining} — `{trade_id}`")
                tps.remove(tp)
                if remaining<=0:
                    update_trade_history(trade_id, {"status":"closed","close_time":now_iso(),"underlying_at_close":f"{last:.2f}"})
                    remove_active_trade(trade_id)
                    if channel: await channel.send(f"📕 **Trade Closed** — `{trade_id}` fully exited."); return
        now_ts = time.time(); b = bucket(now_ts)
        if cur_bucket != b:
            if cur_bucket is not None and c is not None and remaining>0:
                sl_hit = (stop_cond=="close_below" and c < stop_level) or (stop_cond=="close_above" and c > stop_level)
                if sl_hit:
                    slice_qty = remaining
                    append_partial({"partial_id": f"{trade_id}-{int(time.time())}","trade_id": trade_id, "type":"stop","timestamp": now_iso(),
                                    "qty": -slice_qty, "fill_price":"", "underlying_price": c,"target_label": f"SL {stop_cond} {stop_level}",
                                    "reason":"close","commission_fee":"", "realized_pnl_$":"", "notes":""})
                    positions.set(trade_id,"remaining_qty",0)
                    update_trade_history(trade_id, {"status":"stopped","close_time":now_iso(),"underlying_at_close":f"{c:.2f}"})
                    remove_active_trade(trade_id)
                    if channel: await channel.send(f"🛑 **Stop Hit** — closed {slice_qty} @ underlying {c:.2f} — `{trade_id}`")
                    return
            cur_bucket=b; o=h=l=c=last
        else:
            h=max(h,last); l=min(l,last); c=last
        await asyncio.sleep(1)

# ---------------- UTIL CLOSERS ----------------
def close_options_by_underlying(underlying: str, type_filter: Optional[str], pct: Optional[int], qty_abs: Optional[int], limit_price: Optional[float]) -> Dict[str, Any]:
    positions_list = sandbox_list_positions()
    matched = []
    for p in positions_list:
        if p.get("class") != "option": continue
        if str(p.get("underlying","")).upper() != underlying.upper(): continue
        opt_type = (p.get("option_type") or "")
        if not opt_type:
            opt_type = "call" if p.get("symbol","")[-1:].upper()=="C" else "put"
        if type_filter and opt_type.lower() != type_filter: continue
        matched.append(p)
    if not matched:
        return {"closed":0,"details":[],"reason":"no_matching_positions"}
    plan = []; remaining_abs = qty_abs or 0
    for p in matched:
        qty = int(abs(int(p.get("quantity",0))))
        if qty <= 0: continue
        if pct is not None: q_close = max(1, (qty * pct)//100)
        elif qty_abs is not None:
            q_close = min(qty, remaining_abs); remaining_abs -= q_close
        else: q_close = qty
        if q_close <= 0: continue
        plan.append((p["symbol"], q_close))
        if qty_abs is not None and remaining_abs <= 0: break
    closed=0; details=[]
    for occ, q in plan:
        if limit_price is not None:
            resp = sandbox_place_option_order(occ, action="sell_to_close", qty=q, order_type="limit", limit_price=limit_price, duration="day")
        else:
            resp = sandbox_place_option_order(occ, action="sell_to_close", qty=q, order_type="market", duration="day")
        details.append({"occ":occ,"qty":q,"resp":resp}); closed += q
    return {"closed":closed,"details":details,"reason":"ok"}

# ---------------- MAIN ----------------
def require_env(k):
    if not os.getenv(k):
        raise SystemExit(f"Missing required env var: {k}")

if __name__ == "__main__":
    for k in ["DISCORD_TOKEN","OPENAI_API_KEY","TRADIER_LIVE_API_KEY","TRADIER_SANDBOX_API_KEY","TRADIER_SANDBOX_ACCOUNT_ID",
              "GOOGLE_SHEET_ID","GOOGLE_SERVICE_ACCOUNT_JSON_TEXT"]:
        require_env(k)
    bot.run(DISCORD_TOKEN)
