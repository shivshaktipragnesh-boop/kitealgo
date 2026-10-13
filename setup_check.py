"""
Pre-flight check for nifty_smartmoney_bot_live.py  (places NO orders)

Run:  python setup_check.py
Checks: packages, environment variables, Angel One login, scrip master download,
        current NIFTY lot size, spot price, and option-chain quote data.
"""
import os, sys, subprocess, datetime as dt

def ok(m):   print(f"  [OK]   {m}")
def fail(m): print(f"  [FAIL] {m}"); 

print("1) Packages")
for pkg in ["smartapi-python", "pyotp", "requests", "logzero", "websocket-client"]:
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])
        ok(pkg)
    except Exception:
        fail(f"could not install {pkg}")

import requests, pyotp
from SmartApi import SmartConnect

print("\n2) Environment variables")
names = ["ANGEL_API_KEY", "ANGEL_CLIENT_ID", "ANGEL_PIN", "ANGEL_TOTP_SECRET"]
missing = [n for n in names if not os.getenv(n)]
if missing:
    fail("missing: " + ", ".join(missing) + "  -> set them (see step 3) and rerun")
    sys.exit(1)
ok("all four secrets found")

print("\n3) Angel One login")
s = SmartConnect(api_key=os.environ["ANGEL_API_KEY"])
r = s.generateSession(os.environ["ANGEL_CLIENT_ID"], os.environ["ANGEL_PIN"],
                      pyotp.TOTP(os.environ["ANGEL_TOTP_SECRET"]).now())
if not r.get("status"):
    fail(f"login failed: {r.get('message')}  (check PIN, TOTP secret, API key, registered static IP)")
    sys.exit(1)
ok("login successful")

print("\n4) Instrument master + lot size")
data = requests.get("https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json", timeout=60).json()
today = dt.date.today()
exp = lambda d: dt.datetime.strptime(d["expiry"], "%d%b%Y").date()
opts = [d for d in data if d["exch_seg"] == "NFO" and d["name"] == "NIFTY" and d["instrumenttype"] == "OPTIDX" and exp(d) >= today]
nearest = min(exp(d) for d in opts)
sample = [d for d in opts if exp(d) == nearest][0]
ok(f"nearest NIFTY expiry: {nearest}")
ok(f"NIFTY lot size in scrip master: {sample['lotsize']}  <-- make LOT_SIZE in the bot match this")

print("\n5) Market data (works best during market hours)")
try:
    spot = float(s.ltpData("NSE", "Nifty 50", "99926000")["data"]["ltp"])
    ok(f"NIFTY spot: {spot}")
    atm = round(spot / 50) * 50
    toks = [d["token"] for d in opts if exp(d) == nearest and int(float(d["strike"]) / 100) == atm]
    q = s.getMarketData("FULL", {"NFO": toks})["data"]["fetched"]
    for x in q:
        ok(f"{x['tradingSymbol']}  LTP {x['ltp']}  OI {x.get('opnInterest')}  Vol {x.get('tradeVolume')}  avgPrice {x.get('avgPrice')}")
except Exception as e:
    fail(f"market data error: {e}")

print("\nDone. If everything above is [OK], you can run: python nifty_smartmoney_bot_live.py")
