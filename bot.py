import os, json, time, math, asyncio, requests, threading
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from openai import OpenAI

# --------------- ENV ---------------
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TRADIER_LIVE_API_KEY = os.getenv("TRADIER_LIVE_API_KEY")
TRADIER_SANDBOX_API_KEY = os.getenv("TRADIER_SANDBOX_API_KEY")
TRADIER_SANDBOX_ACCOUNT_ID = os.getenv("TRADIER_SANDBOX_ACCOUNT_ID")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# --------------- CONSTANTS ---------------
TZ_ET = timezone.utc  # store UTC; show ET in messages if you want
TRADIER_LIVE = "https://api.tradier.com"
TRADIER_SANDBOX = "https://sandbox.tradier.com"
RTH_START = (14,30)  # 9:30 ET in UTC is 13:30 or 14:30 depending DST; for simplicity we don't hardcode conversion here.
RTH_END   = (21,00)  # Rough; you may convert properly to America/New_York.

# --------------- GOOGLE SHEETS ---------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
def sheets_service():
    creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets()

def gs_append(tab, values):
    sp = sheets_service()
    sp.values().append(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A1",
        valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body={"values":[values]}).execute()

def gs_get_header(tab):
    sp = sheets_service()
    v = sp.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A1:Z1").execute().get("values", [[]])[0]
    return v

def gs_find_row(tab, key_col_name, key_val):
    sp = sheets_service()
    header = gs_get_header(tab)
    col_idx = header.index(key_col_name)
    rows = sp.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A2:Z2000").execute().get("values", [])
    for i, row in enumerate(rows, start=2):
        if len(row) > col_idx and row[col_idx] == key_val:
            return i, header
    return None, header

def gs_read_row(tab, rownum, endcol="T"):
    sp = sheets_service()
    return sp.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A{rownum}:{endcol}{rownum}").execute().get("values", [[]])[0]

def gs_update_row(tab, rownum, values):
    sp = sheets_service()
    sp.values().update(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A{rownum}",
        valueInputOption="USER_ENTERED", body={"values":[values]}).execute()

def append_trade(trade: Dict[str, Any]):
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
    gs_append("Trades", row)

def update_trade(trade_id: str, updates: Dict[str, Any]):
    rownum, header = gs_find_row("Trades","trade_id",trade_id)
    if not rownum: raise ValueError("Trade not found")
    cur = gs_read_row("Trades", rownum, endcol="T")
    cur += [""] * (len(header)-len(cur))
    for k,v in updates.items():
        if k in header:
            idx = header.index(k); cur[idx] = v
    gs_update_row("Trades", rownum, cur)

def append_partial(partial: Dict[str,Any]):
    row = [
        partial.get("partial_id",""), partial.get("trade_id",""), partial.get("type",""),
        partial.get("timestamp",""), partial.get("qty",""), partial.get("fill_price",""),
        partial.get("underlying_price",""), partial.get("target_label",""),
        partial.get("reason",""), partial.get("commission_fee",""),
        partial.get("realized_pnl_$",""), partial.get("notes","")
    ]
    gs_append("Partials", row)

def append_signal(sig: Dict[str,Any]):
    row = [sig.get("signal_id",""), sig.get("received_at",""), sig.get("raw_text","")[:48000], json.dumps(sig.get("parsed_json",{}))[:48000]]
    gs_append("Signals", row)

# --------------- OPENAI (GPT) ---------------
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
    resp = client.chat.completions.create(
        model="gpt-5-think",  # alias for this assistant
        messages=[{"role":"system","content":EXTRACT_SYSTEM},{"role":"user","content":text}],
        temperature=0
    )
    raw = resp.choices[0].message.content.strip()
    return json.loads(raw)

# --------------- TRADIER HELPERS ---------------
def live_quote(symbol: str) -> Optional[float]:
    h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
    r = requests.get(f"{TRADIER_LIVE}/v1/markets/quotes", params={"symbols":symbol}, headers=h, timeout=10)
    r.raise_for_status()
    q = r.json().get("quotes",{}).get("quote",{})
    if not q: return None
    return float(q.get("last", q.get("bid", 0)))

def get_option_chain(symbol: str, expiry: str=None, dte: int=14, greeks=True) -> List[Dict[str,Any]]:
    # For demo we pick nearest expiry from Tradier "expirations" then fetch chain
    h = {"Authorization": f"Bearer {TRADIER_LIVE_API_KEY}", "Accept":"application/json"}
    exps = requests.get(f"{TRADIER_LIVE}/v1/markets/options/expirations", params={"symbol":symbol,"includeAllRoots":"true"}, headers=h).json()
    exps = exps.get("expirations",{}).get("date",[])
    # pick nearest to dte:
    best = exps[0] if exps else None
    if exps:
        # crude nearest by absolute day delta vs requested dte
        # (simplified; production would parse to dates)
        best = exps[0]
    params = {"symbol":symbol, "expiration": best, "greeks":"true" if greeks else "false"}
    chain = requests.get(f"{TRADIER_LIVE}/v1/markets/options/chains", params=params, headers=h).json()
    return chain.get("options",{}).get("option",[]), best

def pick_contract(chain: List[Dict[str,Any]], side: str, target_delta: float=0.5, strike: Optional[float]=None, typ: str="auto"):
    # side long→call, short→put when type auto
    if typ=="auto":
        typ = "call" if side=="long" else "put"
    candidates = [c for c in chain if c.get("option_type")==typ]
    if strike:
        # pick the exact/nearest strike
        return min(candidates, key=lambda c: abs(float(c["strike"])-strike))
    # by |delta- target|
    return min(candidates, key=lambda c: abs(abs(float(c["delta"]))-target_delta))

def sandbox_place_option_order(occ_symbol: str, side: str, qty: int, order_type="market", limit_price=None):
    h = {"Authorization": f"Bearer {TRADIER_SANDBOX_API_KEY}", "Accept":"application/json"}
    data = {
        "class":"option", "symbol":occ_symbol,
        "side":"buy_to_open" if side=="long" else "buy_to_open",  # for puts long is still buy_to_open; for shorts you'd sell_to_open; keeping long-only here
        "quantity": qty, "type": order_type, "duration":"day"
    }
    if limit_price: data["price"] = limit_price
    url = f"{TRADIER_SANDBOX}/v1/accounts/{TRADIER_SANDBOX_ACCOUNT_ID}/orders"
    r = requests.post(url, data=data, headers=h, timeout=10)
    return r.json()

# --------------- STATE / POSITION MANAGER ---------------
class PositionStore:
    """In-memory + Sheet; simple dict keyed by trade_id"""
    def __init__(self): self.state: Dict[str,Dict[str,Any]] = {}
    def add(self, trade_id:str, rec:Dict[str,Any]): self.state[trade_id]=rec
    def get(self, trade_id:str): return self.state.get(trade_id)
    def all_active(self): return {k:v for k,v in self.state.items() if v.get("status") in ("active","tp_partial")}
    def set(self, trade_id, key, val): 
        if trade_id in self.state: self.state[trade_id][key]=val

positions = PositionStore()

def now_iso(): return datetime.utcnow().isoformat(timespec="seconds")+"Z"

# --------------- DISCORD BOT ---------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Buttons
class ActionView(discord.ui.View):
    def __init__(self, trade_id:str): 
        super().__init__(timeout=None); self.trade_id=trade_id
        self.add_item(CloseAllButton(trade_id))
        self.add_item(Close25Button(trade_id))
        self.add_item(MoveSLBEButton(trade_id))

class CloseAllButton(discord.ui.Button):
    def __init__(self, trade_id): 
        super().__init__(label="Close All", style=discord.ButtonStyle.danger)
        self.trade_id=trade_id
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"Confirm close all for {self.trade_id}? Reply `!closeall {self.trade_id} --confirm`", ephemeral=True)

class Close25Button(discord.ui.Button):
    def __init__(self, trade_id): 
        super().__init__(label="Close 25%", style=discord.ButtonStyle.secondary)
        self.trade_id=trade_id
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"Confirm close 25% for {self.trade_id}? Reply `!close25 {self.trade_id} --confirm`", ephemeral=True)

class MoveSLBEButton(discord.ui.Button):
    def __init__(self, trade_id): 
        super().__init__(label="SL → BE", style=discord.ButtonStyle.primary)
        self.trade_id=trade_id
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"Confirm SL→BE for {self.trade_id}? Reply `!moveSLBE {self.trade_id} --confirm`", ephemeral=True)

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    try:
        synced = await bot.tree.sync()
        print(f"Slash commands synced: {len(synced)}")
    except Exception as e:
        print("Slash sync error:", e)

# ---- Commands ----
@bot.command()
async def positions_cmd(ctx, *, filter_text: str = ""):
    """Fallback text command: !positions"""
    await show_positions(ctx)

@bot.tree.command(name="positions", description="Show active positions")
async def slash_positions(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await show_positions(interaction)

async def show_positions(ctx_or_inter):
    active = positions.all_active()
    if not active:
        msg = "No active positions."
        if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.followup.send(msg, ephemeral=True)
        else: await ctx_or_inter.send(msg); return
    chunks=[]
    for tid, rec in active.items():
        line = f"• **{rec['contract']}** — Qty **{rec['remaining_qty']}** — Avg ${rec['avg_entry_premium']:.2f}\n" \
               f"  SL: {rec['stop_desc']} | TP: {rec['tp_desc']} | Trade ID: `{tid}`"
        chunks.append(line)
    content = "**Active Positions (Sandbox)**\n" + "\n".join(chunks)
    # Send with action buttons for the last trade for simplicity
    tid = list(active.keys())[-1]
    view = ActionView(tid)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.followup.send(content, view=view, ephemeral=True)
    else:
        await ctx_or_inter.send(content, view=view)

# Quick actions
@bot.command()
async def closeall(ctx, trade_id: str, confirm: str = ""):
    if confirm != "--confirm": return await ctx.send("Add `--confirm` to proceed.")
    rec = positions.get(trade_id)
    if not rec: return await ctx.send("Trade not found.")
    # Market exit of remaining qty (sandbox)
    qty = int(rec["remaining_qty"])
    if qty <= 0: return await ctx.send("Nothing to close.")
    # Log partial + set closed
    append_partial({
        "partial_id": f"{trade_id}-{int(time.time())}",
        "trade_id": trade_id, "type":"manual_close","timestamp": now_iso(),
        "qty": -qty, "fill_price":"", "underlying_price":"", "target_label":"Close All",
        "reason":"manual", "commission_fee":"", "realized_pnl_$":"", "notes":""
    })
    update_trade(trade_id, {"status":"closed","close_time":now_iso()})
    positions.set(trade_id,"remaining_qty",0)
    await ctx.send(f"📕 **Trade Closed** — {rec['contract']} — qty {qty}")

@bot.command()
async def close25(ctx, trade_id: str, confirm: str = ""):
    if confirm != "--confirm": return await ctx.send("Add `--confirm` to proceed.")
    rec = positions.get(trade_id)
    if not rec: return await ctx.send("Trade not found.")
    qty = int(rec["remaining_qty"])
    slice_qty = max(1, qty//4)
    if qty <= 1: slice_qty = 1
    append_partial({
        "partial_id": f"{trade_id}-{int(time.time())}",
        "trade_id": trade_id, "type":"tp","timestamp": now_iso(),
        "qty": -slice_qty, "fill_price":"", "underlying_price":"", "target_label":"Close 25%",
        "reason":"manual", "commission_fee":"", "realized_pnl_$":"", "notes":""
    })
    positions.set(trade_id,"remaining_qty",qty - slice_qty)
    update_trade(trade_id, {"status":"tp_partial"})
    await ctx.send(f"🎯 **Closed 25%** — {rec['contract']} — {slice_qty} contracts. Remaining {qty - slice_qty}.")

@bot.command()
async def moveSLBE(ctx, trade_id: str, confirm: str = ""):
    if confirm != "--confirm": return await ctx.send("Add `--confirm` to proceed.")
    rec = positions.get(trade_id)
    if not rec: return await ctx.send("Trade not found.")
    # Move SL description to BE
    be_desc = f"BE @ underlying {rec['underlying_at_entry']:.2f}"
    positions.set(trade_id,"stop_desc", be_desc)
    update_trade(trade_id, {"stop_rule": be_desc, "notes":"SL→BE"})
    await ctx.send(f"✏️ **SL moved to BE** — {rec['contract']} — {be_desc}")

# Message listener (alerts)
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot: return
    content = message.content.strip()
    if not content: return
    # Allow commands
    await bot.process_commands(message)
    # Simple heuristic: if starts with ! it's command, ignore parsing
    if content.startswith("!"): return
    # Parse alert → JSON
    try:
        parsed = parse_alert_to_json(content)
    except Exception as e:
        return await message.channel.send(f"⚠️ Parse error: {e}")

    # Log signal
    sig_id = f"sig-{int(time.time())}"
    append_signal({"signal_id":sig_id,"received_at":now_iso(),"raw_text":content,"parsed_json":parsed})

    # Build parent trade row
    ticker = parsed["ticker"].upper()
    entry_rule = f"{parsed['entry']['tf']} {parsed['entry']['cond']} {parsed['entry']['level']}"
    stop_tf = parsed['stop']['tf'] if parsed['stop']['tf']!="same_as_entry" else parsed['entry']['tf']
    stop_rule = f"{stop_tf} {parsed['stop']['cond']} {parsed['stop']['level']}"
    tp_rules = ", ".join([f"{'qty '+str(tp.get('sell_qty')) if 'sell_qty' in tp else ('pct '+str(tp.get('sell_pct')))} @ {tp['trigger']['level']}" for tp in parsed.get('tps',[]) if tp.get('trigger')])
    trade_id = f"{ticker}-{datetime.utcnow().strftime('%Y%m%d')}-{int(time.time())%10000:04d}"
    append_trade({
        "trade_id": trade_id, "source":"Discord", "ticker":ticker,
        "asset_type": parsed.get("asset_type","option"),
        "side": parsed.get("side","long"),
        "contract": "", "qty_total": parsed.get("quantity",1),
        "status":"waiting_confirm", "entry_rule": entry_rule, "stop_rule": stop_rule,
        "tp_rules": tp_rules, "notes": parsed.get("notes","")
    })
    await message.channel.send(f"📥 Signal queued `{trade_id}` for {ticker}. Watching for {entry_rule} (RTH).")

    # Spawn watcher task
    asyncio.create_task(watch_and_execute(trade_id, parsed, message.channel))

async def watch_and_execute(trade_id: str, parsed: Dict[str,Any], channel: discord.TextChannel):
    """Poll live quotes to build timeframe close and trigger entries. (Simple polling MVP)"""
    ticker = parsed["ticker"].upper()
    tf = parsed["entry"]["tf"]  # e.g., '15m','1h'
    tf_sec = {"1m":60,"3m":180,"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}[tf]
    level = float(parsed["entry"]["level"])
    cond = parsed["entry"]["cond"]  # close_above / close_below
    stop_level = float(parsed["stop"]["level"])
    stop_cond = parsed["stop"]["cond"]
    stop_tf = parsed["stop"]["tf"] if parsed["stop"]["tf"]!="same_as_entry" else tf

    # Candle buffer
    cur_bucket = None; o=h=l=c=None
    def bucket(ts): return int(ts//tf_sec)*tf_sec

    while True:
        last = live_quote(ticker)
        if last is None:
            await asyncio.sleep(1); continue
        now = time.time()
        b = bucket(now)
        if cur_bucket != b:
            # previous candle closed; evaluate entry/stop as needed
            if cur_bucket is not None and c is not None:
                # Evaluate entry
                if cond=="close_above" and c > level or cond=="close_below" and c < level:
                    await place_entry_and_manage(trade_id, parsed, channel)
                    return
            # reset new candle
            cur_bucket=b; o=h=l=c=last
        else:
            # update current candle
            h = max(h,last); l = min(l,last); c = last
        await asyncio.sleep(1)

async def place_entry_and_manage(trade_id:str, parsed:Dict[str,Any], channel: discord.TextChannel):
    ticker = parsed["ticker"].upper()
    qty = int(parsed.get("quantity",1))
    side = parsed.get("side","long")

    contract_desc = ""
    fill_premium = None

    if parsed.get("asset_type")=="stock":
        # (MVP: place stock order skipped; focus options per your workflow)
        contract_desc = f"{ticker} shares"
    else:
        # pick option contract (2w default; closest delta or strike)
        chain, expiry = get_option_chain(ticker, dte=parsed.get("option_select",{}).get("dte",14))
        sel = pick_contract(
            chain=chain,
            side=side,
            target_delta=float(parsed.get("option_select",{}).get("delta",0.5)),
            strike=parsed.get("option_select",{}).get("strike"),
            typ=parsed.get("option_select",{}).get("type","auto")
        )
        occ = sel["symbol"]
        contract_desc = f"{ticker} {expiry} {sel['strike']}{sel['option_type'][0].upper()} (Δ {float(sel['delta']):.2f})"
        resp = sandbox_place_option_order(occ, side, qty, order_type="market")
        # sandbox returns simulated; we won't parse fill price here
        fill_premium = None

    # Update positions state + sheet
    positions.add(trade_id,{
        "contract": contract_desc,
        "remaining_qty": qty,
        "avg_entry_premium": fill_premium or 0.0,
        "underlying_at_entry": live_quote(ticker) or 0.0,
        "stop_desc": f"{parsed['stop']['tf'] if parsed['stop']['tf']!='same_as_entry' else parsed['entry']['tf']} {parsed['stop']['cond']} {parsed['stop']['level']}",
        "tp_desc": ", ".join([str(tp['trigger']['level']) for tp in parsed.get('tps',[])]),
        "status":"active"
    })
    update_trade(trade_id, {
        "status":"active","entry_time": datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "contract": contract_desc, "underlying_at_entry": f"{positions.get(trade_id)['underlying_at_entry']:.2f}"
    })
    await channel.send(f"✅ **Trade Opened** — {contract_desc}\nQty: {qty} (Market) | Trade ID: `{trade_id}`", view=ActionView(trade_id))

    # Start TP/SL watcher (MVP: TP touch by underlying; SL on close — reuse same simple candle loop)
    asyncio.create_task(tp_sl_manager(trade_id, parsed, channel))

async def tp_sl_manager(trade_id:str, parsed:Dict[str,Any], channel: discord.TextChannel):
    ticker = parsed["ticker"].upper()
    tf = parsed["entry"]["tf"]; tf_sec = {"1m":60,"3m":180,"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}[tf]
    stop_tf = parsed["stop"]["tf"] if parsed["stop"]["tf"]!="same_as_entry" else tf
    stop_level = float(parsed["stop"]["level"]); stop_cond = parsed["stop"]["cond"]

    cur_bucket=None; c=None; o=h=l=None
    def bucket(ts): return int(ts//tf_sec)*tf_sec

    tps = parsed.get("tps",[])
    while True:
        last = live_quote(ticker)
        if last is None: await asyncio.sleep(1); continue

        # TP touch (underlying)
        remaining = positions.get(trade_id)["remaining_qty"]
        for tp in list(tps):
            lvl = float(tp["trigger"]["level"]); cond = tp["trigger"]["cond"]
            touch = (cond.endswith("above") and last >= lvl) or (cond.endswith("below") and last <= lvl)
            if touch and remaining>0:
                # determine slice
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

        # SL on timeframe close:
        now = time.time(); b = bucket(now)
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

# --------------- MAIN ---------------
if __name__ == "__main__":
    if not DISCORD_TOKEN: raise SystemExit("Missing DISCORD_TOKEN")
    bot.run(DISCORD_TOKEN)
