import http from "node:http";

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
    if (!response.ok) throw new Error(`Kalshi returned HTTP ${response.status}`);

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

const server = http.createServer(async (req, res) => {
  if (req.url === "/health") {
    return send(res, 200, {
      ok: true,
      cached_at: cached?.refreshed_at || null,
      cache_seconds: CACHE_MS / 1000,
    });
  }

  const suppliedToken = req.headers["x-relay-token"];
  if (!RELAY_TOKEN || suppliedToken !== RELAY_TOKEN) {
    return send(res, 401, { error: "Unauthorized" });
  }

  if (!req.url?.startsWith("/markets")) {
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

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Kalshi relay listening on port ${PORT}`);
});
