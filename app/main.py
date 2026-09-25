import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

import httpx
import websockets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

state: Dict[str, Any] = {
    "sources": {
        "binance": {"status": "connecting", "last_update": None},
        "okx": {"status": "connecting", "last_update": None},
        "coinbase": {"status": "connecting", "last_update": None},
    },
    "quotes": {},
}

def now_ms():
    return int(time.time() * 1000)

def touch(source):
    state["sources"][source]["status"] = "online"
    state["sources"][source]["last_update"] = now_ms()

async def binance_worker():
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(
        a.lower() + "@ticker" for a in ASSETS
    )
    while True:
        try:
            state["sources"]["binance"]["status"] = "connecting"
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for raw in ws:
                    msg = json.loads(raw)
                    d = msg.get("data", {})
                    symbol = d.get("s")
                    price = d.get("c")
                    if symbol and price:
                        state["quotes"].setdefault(symbol, {})["binance"] = {
                            "price": float(price), "ts": now_ms()
                        }
                        touch("binance")
        except Exception as e:
            state["sources"]["binance"]["status"] = "offline"
            await asyncio.sleep(3)

async def okx_worker():
    url = "wss://ws.okx.com:8443/ws/v5/public"
    args = [{"channel": "tickers", "instId": a.replace("USDT", "-USDT")} for a in ASSETS]
    while True:
        try:
            state["sources"]["okx"]["status"] = "connecting"
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                async for raw in ws:
                    msg = json.loads(raw)
                    for d in msg.get("data", []):
                        inst = d.get("instId", "")
                        symbol = inst.replace("-", "")
                        if symbol in ASSETS and d.get("last"):
                            state["quotes"].setdefault(symbol, {})["okx"] = {
                                "price": float(d["last"]), "ts": now_ms()
                            }
                            touch("okx")
        except Exception:
            state["sources"]["okx"]["status"] = "offline"
            await asyncio.sleep(3)

async def coinbase_worker():
    url = "wss://advanced-trade-ws.coinbase.com"
    products = [a.replace("USDT", "-USD") for a in ASSETS]
    while True:
        try:
            state["sources"]["coinbase"]["status"] = "connecting"
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": products,
                    "channel": "ticker",
                }))
                async for raw in ws:
                    msg = json.loads(raw)
                    for e in msg.get("events", []):
                        for t in e.get("tickers", []):
                            symbol = t.get("product_id", "").replace("-", "")
                            if symbol in ASSETS and t.get("price"):
                                state["quotes"].setdefault(symbol, {})["coinbase"] = {
                                    "price": float(t["price"]), "ts": now_ms()
                                }
                                touch("coinbase")
        except Exception:
            state["sources"]["coinbase"]["status"] = "offline"
            await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(binance_worker()),
        asyncio.create_task(okx_worker()),
        asyncio.create_task(coinbase_worker()),
    ]
    yield
    for t in tasks:
        t.cancel()

app = FastAPI(title="Lighthouse Data Hub", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"ok": True, "service": "lighthouse-data-hub", "ts": now_ms()}

@app.get("/api/market")
async def market():
    return {
        "ts": now_ms(),
        "sources": state["sources"],
        "quotes": state["quotes"],
        "stocks": {
            "AMD": {"status": "provider_required"},
            "MRVL": {"status": "provider_required"},
            "NVDA": {"status": "provider_required"},
        },
    }
