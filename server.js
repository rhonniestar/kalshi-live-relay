import http from "node:http";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const PORT = Number(process.env.PORT || 3000);
const RELAY_TOKEN = process.env.RELAY_TOKEN || "";
const CACHE_MS = 15_000;
const MAX_CLOSE_WINDOW_SECONDS = 3 * 60 * 60;

let cached = null;
let cachedAt = 0;
let pending = null;

function send(res, status, body) {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "private, no-store",
    "x-content-type-options": "nosniff",
  });
  res.end(JSON.stringify(body));
}

async function fetchMarkets() {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const minClose = nowSeconds - 300;
  const maxClose = nowSeconds + MAX_CLOSE_WINDOW_SECONDS;
  let cursor = "";
  const markets = [];

  do {
    const url = new URL("https://api.elections.kalshi.com/trade-api/v2/markets");
    url.searchParams.set("min_close_ts", String(minClose));
    url.searchParams.set("max_close_ts", String(maxClose));
    url.searchParams.set("mve_filter", "exclude");
    url.searchParams.set("limit", "1000");
    if (cursor) url.searchParams.set("cursor", cursor);

    const response = await fetch(url, {
      headers: { accept: "application/json", "user-agent": "kalshi-private-relay/1.0" },
      signal: AbortSignal.timeout(12_000),
    });
    if (!response.ok) { await response.body?.cancel(); throw new Error(`Kalshi returned HTTP ${response.status}`); }

    const page = await response.json();
    markets.push(...(page.markets || []).filter((market) => market.status === "active"));
    cursor = page.cursor || "";
  } while (cursor);

  if (!markets.length) throw new Error("Kalshi returned no active markets");

  return {
    markets,
    refreshed_at: new Date().toISOString(),
    source: "kalshi",
  };
}

async function getMarkets() {
  if (cached && Date.now() - cachedAt < CACHE_MS) return cached;
  if (!pending) {
    pending = fetchMarkets()
      .then((data) => {
        cached = data;
        cachedAt = Date.now();
        return data;
      })
      .finally(() => {
        pending = null;
      });
  }
  return pending;
}

const upstreamCache = new Map();
const upstreamFlights = new Map();

// Read-only public data endpoints. No arbitrary hosts, accounts, orders or writes.
export function relayTarget(url) {
  const path=url.pathname;
  if (/^\/kalshi\/(?:markets(?:\/[A-Z0-9][A-Z0-9_.:-]{0,160}(?:\/orderbook)?)?|series(?:\/[A-Z0-9][A-Z0-9_.:-]{0,160})?)$/.test(path)) {
    const allowed=new Set(["limit","cursor","min_close_ts","max_close_ts","mve_filter","depth","series_ticker","status"]);
    if ([...url.searchParams.keys()].some(key=>!allowed.has(key))||url.search.length>2400) return null;
    return {url:"https://api.elections.kalshi.com/trade-api/v2"+path.slice(7)+url.search,ttl:path.includes("/series")?300000:path.endsWith("/orderbook")?2000:5000};
  }
  if (/^\/coinbase\/products\/(?:BTC|ETH|SOL|XRP|DOGE|LTC|BCH|ADA|AVAX|LINK|DOT|SHIB|ZEC)-USD\/(?:ticker|candles)$/.test(path)) {
    if(path.endsWith("/ticker")&&url.search)return null;
    if(path.endsWith("/candles")){
      if(url.searchParams.get("granularity")!=="60"||[...url.searchParams.keys()].some(key=>!["granularity","start","end"].includes(key)))return null;
      const start=url.searchParams.get("start"),end=url.searchParams.get("end");
      if(start||end){const a=Date.parse(start||""),b=Date.parse(end||"");if(!Number.isFinite(a)||!Number.isFinite(b)||b<=a||b-a>300*60000)return null;}
    }
    return {url:"https://api.exchange.coinbase.com"+path.slice(9)+url.search,ttl:path.endsWith("/candles")?45000:5000};
  }
  if (/^\/kraken\/0\/public\/(?:Trades|OHLC)$/.test(path)) {
    const pair=url.searchParams.get("pair");
    if(!/^(?:XBT|ETH|SOL|XRP|XDG|LTC|BCH|ADA|AVAX|LINK|DOT|BNB|SHIB|ZEC)USD$/.test(pair||""))return null;
    const trades=path.endsWith("/Trades");
    const allowed=trades?["pair","count"]:["pair","interval"];
    if([...url.searchParams.keys()].some(key=>!allowed.includes(key)))return null;
    if(trades?url.searchParams.get("count")!=="1":url.searchParams.get("interval")!=="1")return null;
    return {url:"https://api.kraken.com"+path.slice(7)+url.search,ttl:trades?3000:45000};
  }
  if (/^\/binance\/api\/v3\/(?:trades|klines)$/.test(path)) {
    if(!/^(?:BTC|ETH|SOL|XRP|DOGE|LTC|BCH|ADA|AVAX|LINK|DOT|BNB|SHIB|ZEC)USDT$/.test(url.searchParams.get("symbol")||""))return null;
    const trades=path.endsWith("/trades"),allowed=trades?["symbol","limit"]:["symbol","limit","interval"];
    if([...url.searchParams.keys()].some(key=>!allowed.includes(key)))return null;
    if(url.searchParams.get("limit")!==(trades?"1":"360")||!trades&&url.searchParams.get("interval")!=="1m")return null;
    return {url:"https://data-api.binance.vision"+path.slice(8)+url.search,ttl:trades?3000:45000};
  }
  if(path==="/coingecko/api/v3/simple/price"&&url.searchParams.get("ids")==="tether"&&url.searchParams.get("vs_currencies")==="usd"&&url.searchParams.get("include_last_updated_at")==="true"&&[...url.searchParams.keys()].every(key=>["ids","vs_currencies","include_last_updated_at"].includes(key))) {
    return {url:"https://api.coingecko.com"+path.slice(10)+url.search,ttl:45000};
  }
  return null;
}

async function getUpstream(target) {
  const hit=upstreamCache.get(target.url);
  if(hit&&Date.now()-hit.at<target.ttl) return hit.body;
  if(upstreamFlights.has(target.url)) return upstreamFlights.get(target.url);
  if(upstreamFlights.size>=30) throw new Error("Too many data requests; retry shortly");
  const flight=(async()=>{
    const started=Date.now();
    const response=await fetch(target.url,{headers:{accept:"application/json","user-agent":"kalshi-private-relay/1.1","cache-control":"no-cache"},signal:AbortSignal.timeout(8000)});
    if(!response.ok){await response.body?.cancel();throw new Error(`Data source returned HTTP ${response.status}`);}
    const data=await response.json();
    if(!data||typeof data!=="object")throw new Error("Invalid source response");
    const serverDate=Date.parse(response.headers.get("date")||""),age=Number(response.headers.get("age")||0);
    const stamp=new Date(Math.min(started,Number.isFinite(serverDate)?serverDate:started,Date.now()-Math.max(0,Number.isFinite(age)?age:0)*1000)).toISOString();
    const body=Array.isArray(data)?{data,refreshed_at:stamp}:{...data,refreshed_at:stamp};
    if(upstreamCache.size>=200)upstreamCache.delete(upstreamCache.keys().next().value);
    upstreamCache.set(target.url,{at:started,body});return body;
  })();
  upstreamFlights.set(target.url,flight);
  try{return await flight;}finally{upstreamFlights.delete(target.url);}
}

export const server = http.createServer(async (req, res) => {
  if (req.url === "/health") {
    return send(res, 200, {
      ok: true,
      version: "multi-source-2",
      cached_at: cached?.refreshed_at || null,
      cache_seconds: CACHE_MS / 1000,
    });
  }

  const suppliedToken = req.headers["x-relay-token"];
  if (!RELAY_TOKEN || suppliedToken !== RELAY_TOKEN) {
    return send(res, 401, { error: "Unauthorized" });
  }

  if(req.method!=="GET")return send(res,405,{error:"Read-only endpoint"});
  const url=new URL(req.url,"http://relay.local");
  const target=relayTarget(url);
  if(target){try{return send(res,200,await getUpstream(target));}catch(error){return send(res,502,{error:error instanceof Error?error.message:"Data request failed"});}}
  if (url.pathname !== "/markets") {
    return send(res, 404, { error: "Not found" });
  }

  try {
    return send(res, 200, await getMarkets());
  } catch (error) {
    if (cached && Date.now() - cachedAt < 2 * 60_000) {
      return send(res, 200, { ...cached, warning: "Serving recent cached data" });
    }
    return send(res, 502, {
      error: error instanceof Error ? error.message : "Relay request failed",
    });
  }
});

if(process.argv[1]&&fileURLToPath(import.meta.url)===resolve(process.argv[1])) server.listen(PORT, "0.0.0.0", () => {
  console.log(`Kalshi relay listening on port ${PORT}`);
});
