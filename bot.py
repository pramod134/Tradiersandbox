# bot.py
# Discord → GPT parser → Live Tradier data triggers → Sandbox orders → Google Sheets logging
# Quick actions: Close All, Close 25%, SL→BE

import os, json, time, math, asyncio, requests, traceback
from datetime import datetime
from typing import Dict, Any, List, Optional

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

TRADES_TAB   = os.getenv("TRADES_TAB", "Trades")
PARTIALS_TAB = os.getenv("PARTIALS_TAB", "Partials")
SIGNALS_TAB  = os.getenv("SIGNALS_TAB", "Signals")

TRADIER_LIVE = "https://api.tradier.com"
TRADIER_SANDBOX = "https://sandbox.tradier.com"

MODEL_PRIMARY = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
MODEL_FALLBACK = os.getenv("OPENAI_MODEL_FALLBACK", "gpt-4.1-mini")

# Timeframes (seconds)
TF_SECONDS = {"1m":60,"3m":180,"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- GOOGLE SHEETS HELPERS ----------------
def sheets_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON_TEXT:
        raise RuntimeError("Set GOOGLE_SERVICE_ACCOUNT_JSON_TEXT to the raw JSON of your service account.")
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Set GOOGLE_SHEET_ID to your Google Sheet ID (string between /d/ and /edit).")
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON_TEXT)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets()

def gs_append(tab, values):
    sp = sheets_service()
    sp.values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values":[values]}
    ).execute()

def gs_get_header(tab):
    sp = sheets_service()
    v = sp.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab}!A1:Z1").execute().get("values", [[]])[0]
    return v

def gs_find_row(tab, key_col_name, key_val):
    sp = sheets_service()
    header = gs_get_header(tab)
    if key_col_name not in header:
        raise RuntimeError(f"Header '{key_col_name}' not found in {tab}")
    col_idx = header.index(key_col_name)
    rows = sp.values().get(spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab}!A2:Z2000").execute().get("values", [])
    for i, row in enumerate(rows, start=2):
        if len(row) > col_idx and row[col_idx] == key_val:
            return i, header
    return None, header

def gs_read_row(tab, rownum, endcol="T"):
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

def append_trade(trade: Dict[str, Any]):
    # Trades columns:
    # trade_id | source | ticker | asset_type | side | contract | qty_total | status | entry_rule | stop_rule | tp_rules
    # | entry_time | entry_price | underlying_at_entry | close_time | close_price | underlying_at_close | realized_pnl_$ | realized_pnl_% | notes
    row = [
        trade.get("trade_id",""), trade.get("source",""), trade.get("ticker",""),
        trade.get("asset_type",""), trade.get("side",""), trade.get("contract",""),
        trade.get("qty_total",""), trade.get("status","waiting_confirm"),
        trade.get("entry_rule",""), trade.get("stop_rule",""), trade.get("tp_rules",""),
        trade.get("entry_time",""), trade.get("entry_price",""),
        trade.get("underlying_at_entry",""), trade.get("close_time",""),
        trade.get("close_price",""), trade.get("underlying_at_close",""),
        "", "", trade.get("notes","")
    ]
    gs_append(TRADES_TAB, row)

def update_trade(trade_id: str, updates: Dict[str, Any]):
    rownum, header = gs_find_row(TRADES_TAB,"trade_id",trade_id)
    if not rownum: raise ValueError("Trade not found")
    cur = gs_read_row(TRADES_TAB, rownum, endcol="T")
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
    row = [
        sig.get("signal_id",""),
        sig.get("received_at",""),
        sig.get("raw_text","")[:48000],
        json.dumps(sig.get("parsed_json", {}))[:48000],
    ]
    gs_append(SIGNALS_TAB, row)

# ---------------- OPENAI (GPT) ----------------
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
 "option_select":{"dte":14,"delta":0.5,"type":"auto|call|put","strike":null,"choose":"nearest_delta|exact"},
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

# ---------------- TRADIER HELPERS ----------------
def live_quote(symbol: str) -> Optional[float]:
    h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
    r = requests.get(f"{TRADIER_LIVE}/v1/markets/quotes", params={"symbols":symbol}, headers=h, timeout=10)
    r.raise_for_status()
    q = r.json().get("quotes",{}).get("quote",{})
    if not q: return None
    return float(q.get("last", q.get("bid", 0)))

def get_option_chain(symbol: str, dte: int=14, greeks=True):
    # MVP: pick first listed expiry; refine later to true nearest DTE
    h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
    exp_resp = requests.get(f"{TRADIER_LIVE}/v1/markets/options/expirations",
                            params={"symbol":symbol,"includeAllRoots":"true"}, headers=h, timeout=10).json()
    exps = exp_resp.get("expirations",{}).get("date",[])
    best = exps[0] if exps else None
    chain = requests.get(f"{TRADIER_LIVE}/v1/markets/options/chains",
                         params={"symbol":symbol,"expiration":best,"greeks":"true" if greeks else "false"},
                         headers=h, timeout=10).json()
    return chain.get("options",{}).get("option",[]), best

def pick_contract(chain: List[Dict[str,Any]], side: str, target_delta: float=0.5, strike: Optional[float]=None, typ: str="auto"):
    if typ=="auto":
        typ = "call" if side=="long" else "put"
    candidates = [c for c in chain if c.get("option_type")==typ]
    if not candidates:
        raise RuntimeError("No matching option_type candidates in chain.")
    if strike is not None:
        return min(candidates, key=lambda c: abs(float(c["strike"])-float(strike)))
    return min(candidates, key=lambda c: abs(abs(float(c.get("delta",0))) - float(target_delta)))

def sandbox_place_option_order(occ_symbol: str, side: str, qty: int, order_type="market", limit_price=None):
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    data = {
        "class":"option",
        "symbol":occ_symbol,
        "side":"buy_to_open",  # MVP: long options only
        "quantity": qty,
        "type": order_type,
        "duration":"day"
    }
    if limit_price: data["price"] = limit_price
    url = f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/orders"
    r = requests.post(url, data=data, headers=h, timeout=15)
    return r.json()

# ---------------- STATE ----------------
class PositionStore:
    def __init__(self): self.state: Dict[str,Dict[str,Any]] = {}
    def add(self, tid:str, rec:Dict[str,Any]): self.state[tid]=rec
    def get(self, tid:str): return self.state.get(tid)
    def all_active(self): return {k:v for k,v in self.state.items() if v.get("status") in ("active","tp_partial")}
    def set(self, tid, key, val):
        if tid in self.state: self.state[tid][key]=val

positions = PositionStore()
def now_iso(): return datetime.utcnow().isoformat(timespec="seconds")+"Z"

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

# Slash: /positions
@bot.tree.command(name="positions", description="Show active positions")
async def slash_positions(interaction: discord.Interaction):
    await show_positions(interaction)

# Slash: /health
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

async def show_positions(ctx_or_inter):
    active = positions.all_active()
    if not active:
        msg = "No active positions."
        if isinstance(ctx_or_inter, discord.Interaction):
            await ctx_or_inter.response.send_message(msg, ephemeral=True)
        else:
            await ctx_or_inter.send(msg)
        return
    lines=[]
    last_tid=None
    for tid, rec in active.items():
        last_tid = tid
        lines.append(
            f"• **{rec['contract']}** — Qty **{rec['remaining_qty']}** — Avg ${rec.get('avg_entry_premium',0):.2f}\n"
            f"  SL: {rec['stop_desc']} | TP: {rec['tp_desc']} | Trade ID: `{tid}`"
        )
    content = "**Active Positions (Sandbox)**\n" + "\n".join(lines)
    view = ActionView(last_tid) if last_tid else None
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(content, ephemeral=True, view=view)
    else:
        await ctx_or_inter.send(content, view=view)

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
    update_trade(trade_id, {"status":"closed","close_time":now_iso()})
    await ctx.send(f"📕 **Trade Closed** — {rec['contract']} — qty {qty}")

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
    update_trade(trade_id, {"status":"tp_partial"})
    await ctx.send(f"🎯 **Closed 25%** — {rec['contract']} — {slice_qty} contracts. Remaining {qty - slice_qty}.")

@bot.command()
async def moveSLBE(ctx, trade_id: str, confirm: str = ""):
    if confirm != "--confirm": return await ctx.send("Add `--confirm` to proceed.")
    rec = positions.get(trade_id)
    if not rec: return await ctx.send("Trade not found.")
    be_desc = f"BE @ underlying {rec.get('underlying_at_entry',0):.2f}"
    positions.set(trade_id,"stop_desc", be_desc)
    update_trade(trade_id, {"stop_rule": be_desc, "notes":"SL→BE"})
    await ctx.send(f"✏️ **SL moved to BE** — {rec['contract']} — {be_desc}")

# ---------------- MESSAGE LISTENER: parse alerts ----------------
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot: return
    content = message.content.strip()
    if not content: return
    await bot.process_commands(message)
    if content.startswith("!"): return

    # Parse plain English → JSON
    try:
        parsed = parse_alert_to_json(content)
    except Exception as e:
        return await message.channel.send(f"⚠️ Parse error: {e}")

    # Log raw signal
    sig_id = f"sig-{int(time.time())}"
    append_signal({"signal_id":sig_id,"received_at":now_iso(),"raw_text":content,"parsed_json":parsed})

    # Build parent trade
    ticker = parsed["ticker"].upper()
    entry_rule = f"{parsed['entry']['tf']} {parsed['entry']['cond']} {parsed['entry']['level']}"
    stop_tf = parsed['stop']['tf'] if parsed['stop']['tf']!="same_as_entry" else parsed['entry']['tf']
    stop_rule = f"{stop_tf} {parsed['stop']['cond']} {parsed['stop']['level']}"
    tp_rules = ", ".join([f"{'qty '+str(tp.get('sell_qty')) if 'sell_qty' in tp else ('pct '+str(tp.get('sell_pct')))} @ {tp['trigger']['level']}" for tp in parsed.get('tps',[]) if tp.get('trigger')])

    trade_id = f"{ticker}-{datetime.utcnow().strftime('%Y%m%d')}-{int(time.time())%100000:05d}"
    append_trade({
        "trade_id": trade_id, "source":"Discord", "ticker":ticker,
        "asset_type": parsed.get("asset_type","option"),
        "side": parsed.get("side","long"),
        "contract": "", "qty_total": parsed.get("quantity",1),
        "status":"waiting_confirm", "entry_rule": entry_rule, "stop_rule": stop_rule,
        "tp_rules": tp_rules, "notes": parsed.get("notes","")
    })
    await message.channel.send(f"📥 Signal queued `{trade_id}` for {ticker}. Watching for {entry_rule} (RTH).")

    # Spawn watcher
    asyncio.create_task(watch_and_execute(trade_id, parsed, message.channel))

# ---------------- WATCHERS ----------------
def _tf_seconds(tf: str) -> int:
    if tf not in TF_SECONDS: raise ValueError(f"Unsupported timeframe: {tf}")
    return TF_SECONDS[tf]

async def watch_and_execute(trade_id: str, parsed: Dict[str,Any], channel: discord.TextChannel):
    ticker = parsed["ticker"].upper()
    tf = parsed["entry"]["tf"]
    tf_sec = _tf_seconds(tf)
    level = float(parsed["entry"]["level"])
    cond = parsed["entry"]["cond"]

    cur_bucket=None; o=h=l=c=None
    bucket = lambda ts: int(ts//tf_sec)*tf_sec

    while True:
        last = live_quote(ticker)
        if last is None:
            await asyncio.sleep(1); continue
        now = time.time()
        b = bucket(now)
        if cur_bucket != b:
            if cur_bucket is not None and c is not None:
                trigger = (cond=="close_above" and c > level) or (cond=="close_below" and c < level)
                if trigger:
                    await place_entry_and_manage(trade_id, parsed, channel)
                    return
            cur_bucket=b; o=h=l=c=last
        else:
            h=max(h,last); l=min(l,last); c=last
        await asyncio.sleep(1)

def get_option_chain(symbol: str, dte: int=14, greeks=True):
    h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
    exp_resp = requests.get(f"{TRADIER_LIVE}/v1/markets/options/expirations",
                            params={"symbol":symbol,"includeAllRoots":"true"}, headers=h, timeout=10).json()
    exps = exp_resp.get("expirations",{}).get("date",[])
    best = exps[0] if exps else None
    chain = requests.get(f"{TRADIER_LIVE}/v1/markets/options/chains",
                         params={"symbol":symbol,"expiration":best,"greeks":"true" if greeks else "false"},
                         headers=h, timeout=10).json()
    return chain.get("options",{}).get("option",[]), best

def pick_contract(chain: List[Dict[str,Any]], side: str, target_delta: float=0.5, strike: Optional[float]=None, typ: str="auto"):
    if typ=="auto":
        typ = "call" if side=="long" else "put"
    candidates = [c for c in chain if c.get("option_type")==typ]
    if not candidates:
        raise RuntimeError("No matching option_type candidates in chain.")
    if strike is not None:
        return min(candidates, key=lambda c: abs(float(c["strike"])-float(strike)))
    return min(candidates, key=lambda c: abs(abs(float(c.get("delta",0))) - float(target_delta)))

def sandbox_place_option_order(occ_symbol: str, side: str, qty: int, order_type="market", limit_price=None):
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    data = {
        "class":"option",
        "symbol":occ_symbol,
        "side":"buy_to_open",
        "quantity": qty,
        "type": order_type,
        "duration":"day"
    }
    if limit_price: data["price"] = limit_price
    url = f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/orders"
    r = requests.post(url, data=data, headers=h, timeout=15)
    return r.json()

async def place_entry_and_manage(trade_id:str, parsed:Dict[str,Any], channel: discord.TextChannel):
    ticker = parsed["ticker"].upper()
    qty = int(parsed.get("quantity",1))
    side = parsed.get("side","long")
    asset_type = parsed.get("asset_type","option")

    contract_desc = ""
    fill_premium = None
    underlying_now = live_quote(ticker) or 0.0

    if asset_type == "stock":
        contract_desc = f"{ticker} shares"
        # TODO: implement stock order via sandbox if needed
    else:
        chain, expiry = get_option_chain(ticker, dte=parsed.get("option_select",{}).get("dte",14))
        sel = pick_contract(
            chain=chain,
            side=side,
            target_delta=float(parsed.get("option_select",{}).get("delta",0.5)),
            strike=parsed.get("option_select",{}).get("strike"),
            typ=parsed.get("option_select",{}).get("type","auto")
        )
        occ = sel["symbol"]
        contract_desc = f"{ticker} {expiry} {sel['strike']}{sel['option_type'][0].upper()} (Δ {float(sel.get('delta',0)):.2f})"
        _ = sandbox_place_option_order(occ, side, qty, order_type="market")

    positions.add(trade_id,{
        "contract": contract_desc,
        "remaining_qty": qty,
        "avg_entry_premium": fill_premium or 0.0,
        "underlying_at_entry": underlying_now,
        "stop_desc": f"{parsed['stop']['tf'] if parsed['stop']['tf']!='same_as_entry' else parsed['entry']['tf']} {parsed['stop']['cond']} {parsed['stop']['level']}",
        "tp_desc": ", ".join([str(tp['trigger']['level']) for tp in parsed.get('tps',[])]),
        "status":"active"
    })
    update_trade(trade_id, {
        "status":"active","entry_time": now_iso(),
        "contract": contract_desc, "underlying_at_entry": f"{underlying_now:.2f}"
    })
    await channel.send(f"✅ **Trade Opened** — {contract_desc}\nQty: {qty} (Market) | Trade ID: `{trade_id}`",
                       view=ActionView(trade_id))

    asyncio.create_task(tp_sl_manager(trade_id, parsed, channel))

async def tp_sl_manager(trade_id:str, parsed:Dict[str,Any], channel: discord.TextChannel):
    ticker = parsed["ticker"].upper()
    tf = parsed["entry"]["tf"]; tf_sec = TF_SECONDS[tf]

    stop_tf = parsed["stop"]["tf"] if parsed["stop"]["tf"]!="same_as_entry" else tf
    stop_level = float(parsed["stop"]["level"]); stop_cond = parsed["stop"]["cond"]

    cur_bucket=None; o=h=l=c=None
    bucket = lambda ts: int(ts//tf_sec)*tf_sec

    tps = parsed.get("tps",[])
    while True:
        last = live_quote(ticker)
        if last is None:
            await asyncio.sleep(1); continue

        # TP (touch on underlying)
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
                remaining -= slice_qty
                positions.set(trade_id,"remaining_qty",remaining)
                append_partial({
                    "partial_id": f"{trade_id}-{int(time.time())}",
                    "trade_id": trade_id, "type":"tp","timestamp": now_iso(),
                    "qty": -slice_qty, "fill_price":"", "underlying_price": last,
                    "target_label": f"TP @ {lvl}", "reason":"touch",
                    "commission_fee":"", "realized_pnl_$":"", "notes":""
                })
                await channel.send(f"🎯 **TP Hit** — {slice_qty} closed @ underlying {last:.2f} — Remaining {remaining} — `{trade_id}`")
                tps.remove(tp)
                if remaining<=0:
                    update_trade(trade_id, {"status":"closed","close_time":now_iso(),"underlying_at_close":f"{last:.2f}"})
                    await channel.send(f"📕 **Trade Closed** — `{trade_id}` fully exited.")
                    return

        # SL on timeframe close
        now_ts = time.time(); b = bucket(now_ts)
        if cur_bucket != b:
            if cur_bucket is not None and c is not None and remaining>0:
                sl_hit = (stop_cond=="close_below" and c < stop_level) or (stop_cond=="close_above" and c > stop_level)
                if sl_hit:
                    slice_qty = remaining
                    append_partial({
                        "partial_id": f"{trade_id}-{int(time.time())}",
                        "trade_id": trade_id, "type":"stop","timestamp": now_iso(),
                        "qty": -slice_qty, "fill_price":"", "underlying_price": c,
                        "target_label": f"SL {stop_cond} {stop_level}", "reason":"close",
                        "commission_fee":"", "realized_pnl_$":"", "notes":""
                    })
                    positions.set(trade_id,"remaining_qty",0)
                    update_trade(trade_id, {"status":"stopped","close_time":now_iso(),"underlying_at_close":f"{c:.2f}"})
                    await channel.send(f"🛑 **Stop Hit** — closed {slice_qty} @ underlying {c:.2f} — `{trade_id}`")
                    return
            cur_bucket=b; o=h=l=c=last
        else:
            h=max(h,last); l=min(l,last); c=last
        await asyncio.sleep(1)

# ---------------- MAIN ----------------
def require_env(k):
    if not os.getenv(k):
        raise SystemExit(f"Missing required env var: {k}")

if __name__ == "__main__":
    for k in [
        "DISCORD_TOKEN","OPENAI_API_KEY",
        "TRADIER_LIVE_API_KEY","TRADIER_SANDBOX_API_KEY","TRADIER_SANDBOX_ACCOUNT_ID",
        "GOOGLE_SHEET_ID","GOOGLE_SERVICE_ACCOUNT_JSON_TEXT"
    ]:
        require_env(k)

    intents = discord.Intents.default()
    intents.message_content = True
    bot.run(DISCORD_TOKEN)
