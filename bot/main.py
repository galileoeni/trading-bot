import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from requests import exceptions as requests_exceptions

load_dotenv()

BASE_URL = os.getenv("MUSASHI_API_BASE_URL", "https://musashi-api.vercel.app").rstrip("/")
BOT_MODE = os.getenv("BOT_MODE", "paper").strip().lower()
SCAN_INTERVAL_SECONDS = int(os.getenv("BOT_SCAN_INTERVAL_SECONDS", "45"))
MIN_CONFIDENCE = float(os.getenv("BOT_MIN_CONFIDENCE", "0.76"))
MIN_EDGE = float(os.getenv("BOT_MIN_EDGE", "0.05"))
MIN_VOLUME_24H = float(os.getenv("BOT_MIN_VOLUME_24H", "20000"))
MIN_PRICE = float(os.getenv("BOT_MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("BOT_MAX_PRICE", "0.85"))
BANKROLL_USD = float(os.getenv("BOT_BANKROLL_USD", "10"))
MAX_POSITION_USD = float(os.getenv("BOT_MAX_POSITION_USD", "3"))
MAX_TOTAL_EXPOSURE_USD = float(os.getenv("BOT_MAX_TOTAL_EXPOSURE_USD", "10"))
LIMIT_ONE_POSITION_PER_EVENT = os.getenv("BOT_LIMIT_ONE_POSITION_PER_EVENT", "true").lower() == "true"
TAKE_PROFIT_PCT = float(os.getenv("BOT_TAKE_PROFIT_PCT", "0.18"))
STOP_LOSS_PCT = float(os.getenv("BOT_STOP_LOSS_PCT", "0.10"))
MAX_HOLD_MINUTES = int(os.getenv("BOT_MAX_HOLD_MINUTES", "240"))
EXIT_ON_SIGNAL_REVERSAL = os.getenv("BOT_EXIT_ON_SIGNAL_REVERSAL", "true").lower() == "true"
EXIT_ORDER_TIMEOUT_SECONDS = int(os.getenv("BOT_EXIT_ORDER_TIMEOUT_SECONDS", "120"))
EXIT_ORDER_REPRICE = os.getenv("BOT_EXIT_ORDER_REPRICE", "true").lower() == "true"
STARTUP_RECONCILE = os.getenv("BOT_STARTUP_RECONCILE", "true").lower() == "true"
POLYMARKET_WS_ENABLED = os.getenv("POLYMARKET_WS_ENABLED", "true").lower() == "true"
RESTRICTED_COUNTRIES = {
    item.strip().upper()
    for item in os.getenv(
        "BOT_RESTRICTED_COUNTRIES",
        "AU,BE,BY,BI,CF,CD,CU,DE,ET,FR,GB,IR,IQ,IT,KP,LB,LY,MM,NI,NL,PL,RU,SG,SO,SS,SD,SY,TH,TW,UM,US,VE,YE,ZW",
    ).split(",")
    if item.strip()
}
RESTRICTED_REGIONS = {
    "CA": {"ON"},
    "UA": {"43", "14", "09"},
}
MUSASHI_CONNECT_TIMEOUT_SECONDS = float(os.getenv("MUSASHI_CONNECT_TIMEOUT_SECONDS", "10"))
MUSASHI_READ_TIMEOUT_SECONDS = float(os.getenv("MUSASHI_READ_TIMEOUT_SECONDS", "30"))

POLYMARKET_HOST = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com").rstrip("/")
POLYMARKET_CHAIN_ID = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))
POLYMARKET_FUNDER = os.getenv("POLYMARKET_FUNDER", "")

DATA_DIR = Path("bot/data")
LOG_DIR = Path("bot/logs")
POSITIONS_FILE = DATA_DIR / "positions.json"
TRADES_FILE = DATA_DIR / "trades.jsonl"
SEEN_FILE = DATA_DIR / "seen_event_ids.json"
PENDING_ORDERS_FILE = DATA_DIR / "pending_orders.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_DIR / "bot.log", mode="w"), logging.StreamHandler()],
)
logger = logging.getLogger("musashi-poly-bot")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def save_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2))


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    with path.open("a") as handle:
        handle.write(json.dumps(value) + "\n")


def extract_health_status(payload: Any) -> str:
    if not isinstance(payload, dict):
        return str(payload)
    for key in ("status", "message", "ok"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("status", "message", "ok"):
            value = data.get(key)
            if value not in (None, ""):
                return str(value)
    return "unknown"


def extract_condition_id(payload: dict[str, Any]) -> str | None:
    for key in ("conditionId", "condition_id", "conditionID", "market"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


class GeolocationClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "musashi-poly-bot/1.0"})

    def locate(self) -> dict[str, Any]:
        providers = [
            ("https://ipinfo.io/json", self._parse_ipinfo),
            ("https://ipapi.co/json/", self._parse_ipapi),
        ]
        for url, parser in providers:
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                payload = response.json()
                location = parser(payload)
                if location:
                    return {
                        "provider": url,
                        "ip": location.get("ip"),
                        "city": location.get("city"),
                        "region": location.get("region"),
                        "country": location.get("country"),
                        "loc": location.get("loc"),
                        "timezone": location.get("timezone"),
                        "raw": payload,
                    }
            except Exception as exc:
                logger.warning("Geolocation lookup failed via %s: %s", url, exc)
        return {}

    @staticmethod
    def _parse_ipinfo(payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "ip": payload.get("ip"),
            "city": payload.get("city"),
            "region": payload.get("region"),
            "country": payload.get("country"),
            "loc": payload.get("loc"),
            "timezone": payload.get("timezone"),
        }

    @staticmethod
    def _parse_ipapi(payload: dict[str, Any]) -> dict[str, Any]:
        loc = None
        latitude = payload.get("latitude")
        longitude = payload.get("longitude")
        if latitude not in (None, "") and longitude not in (None, ""):
            loc = f"{latitude},{longitude}"
        return {
            "ip": payload.get("ip"),
            "city": payload.get("city"),
            "region": payload.get("region"),
            "country": payload.get("country_name") or payload.get("country"),
            "loc": loc,
            "timezone": payload.get("timezone"),
        }


class SafetyShutdown(RuntimeError):
    pass


class PolymarketMarketStream:
    def __init__(self, enabled: bool = True) -> None:
        self.url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self.enabled = enabled
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._quotes: dict[str, dict[str, Any]] = {}
        self._asset_ids: set[str] = set()
        self._ws: Any = None

    def start(self) -> None:
        if not self.enabled or self._thread:
            return
        self._thread = threading.Thread(target=self._run, name="polymarket-market-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def set_assets(self, asset_ids: set[str]) -> None:
        with self._lock:
            self._asset_ids = {str(asset_id) for asset_id in asset_ids if asset_id}

    def get_quote(self, asset_id: str) -> dict[str, Any] | None:
        with self._lock:
            quote = self._quotes.get(str(asset_id))
            return dict(quote) if quote else None

    def _run(self) -> None:
        try:
            import websocket
        except ImportError:
            logger.warning("websocket-client is not installed; Polymarket market WebSocket disabled")
            self.enabled = False
            return

        while not self._stop_event.is_set():
            asset_ids = self._snapshot_assets()
            if not asset_ids:
                time.sleep(1)
                continue

            try:
                ws = websocket.create_connection(self.url, timeout=10)
                ws.settimeout(1)
                self._ws = ws
                subscribed_assets = set(asset_ids)
                ws.send(json.dumps({"assets_ids": sorted(subscribed_assets), "type": "market"}))
                last_ping_at = time.time()

                while not self._stop_event.is_set():
                    current_assets = self._snapshot_assets()
                    self._sync_asset_subscription(ws, subscribed_assets, current_assets)
                    subscribed_assets = current_assets
                    if time.time() - last_ping_at >= 8:
                        ws.send("PING")
                        last_ping_at = time.time()
                    try:
                        message = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not message or message == "PONG":
                        continue
                    self._handle_message(message)
            except Exception as exc:
                logger.warning("Polymarket market WebSocket reconnecting after error: %s", exc)
                time.sleep(2)
            finally:
                if self._ws is not None:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

    def _snapshot_assets(self) -> set[str]:
        with self._lock:
            return set(self._asset_ids)

    def _sync_asset_subscription(self, ws: Any, subscribed_assets: set[str], current_assets: set[str]) -> None:
        add_assets = sorted(current_assets - subscribed_assets)
        remove_assets = sorted(subscribed_assets - current_assets)
        if add_assets:
            ws.send(json.dumps({"operation": "subscribe", "assets_ids": add_assets}))
        if remove_assets:
            ws.send(json.dumps({"operation": "unsubscribe", "assets_ids": remove_assets}))

    def _handle_message(self, message: str) -> None:
        payload = json.loads(message)
        events = payload if isinstance(payload, list) else [payload]
        for event in events:
            if not isinstance(event, dict):
                continue
            asset_id = str(event.get("asset_id") or event.get("assetId") or "")
            if not asset_id:
                continue
            quote_update = self._parse_quote_event(event)
            if quote_update:
                with self._lock:
                    previous = self._quotes.get(asset_id, {})
                    self._quotes[asset_id] = {**previous, **quote_update, "updated_at": utc_now_iso()}

    def _parse_quote_event(self, event: dict[str, Any]) -> dict[str, Any]:
        bids = event.get("bids") or []
        asks = event.get("asks") or []
        best_bid = self._best_price(bids, prefer_max=True)
        best_ask = self._best_price(asks, prefer_max=False)
        last_trade = as_float(
            event.get("last_trade_price"),
            as_float(event.get("lastTradePrice"), as_float(event.get("price"))),
        )
        mid = None
        if best_bid > 0 and best_ask > 0:
            mid = round((best_bid + best_ask) / 2, 4)
        reference_price = mid or last_trade or best_bid or best_ask
        if reference_price <= 0:
            return {}
        return {
            "best_bid": clamp_price(best_bid) if best_bid > 0 else None,
            "best_ask": clamp_price(best_ask) if best_ask > 0 else None,
            "last_trade_price": clamp_price(last_trade) if last_trade > 0 else None,
            "mid_price": clamp_price(mid) if mid else None,
            "reference_price": clamp_price(reference_price),
        }

    @staticmethod
    def _best_price(levels: list[dict[str, Any]], prefer_max: bool) -> float:
        prices = [as_float(level.get("price")) for level in levels if as_float(level.get("price")) > 0]
        if not prices:
            return 0.0
        return max(prices) if prefer_max else min(prices)


class PolymarketUserStream:
    def __init__(self, api_creds: dict[str, str], enabled: bool = True) -> None:
        self.url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        self.enabled = enabled
        self.api_creds = api_creds
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._condition_ids: set[str] = set()
        self._order_events: dict[str, dict[str, Any]] = {}
        self._ws: Any = None

    def start(self) -> None:
        if not self.enabled or self._thread:
            return
        self._thread = threading.Thread(target=self._run, name="polymarket-user-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def set_markets(self, condition_ids: set[str]) -> None:
        with self._lock:
            self._condition_ids = {str(condition_id) for condition_id in condition_ids if condition_id}

    def has_update_for_order(self, order_id: str) -> bool:
        with self._lock:
            return str(order_id) in self._order_events

    def pop_order_event(self, order_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._order_events.pop(str(order_id), None)

    def _run(self) -> None:
        try:
            import websocket
        except ImportError:
            logger.warning("websocket-client is not installed; Polymarket user WebSocket disabled")
            self.enabled = False
            return

        while not self._stop_event.is_set():
            condition_ids = self._snapshot_condition_ids()
            if not condition_ids:
                time.sleep(1)
                continue

            try:
                ws = websocket.create_connection(self.url, timeout=10)
                ws.settimeout(1)
                self._ws = ws
                subscribed_ids = set(condition_ids)
                ws.send(json.dumps({**self.api_creds, "markets": sorted(subscribed_ids), "type": "user"}))
                last_ping_at = time.time()

                while not self._stop_event.is_set():
                    current_ids = self._snapshot_condition_ids()
                    self._sync_condition_subscription(ws, subscribed_ids, current_ids)
                    subscribed_ids = current_ids
                    if time.time() - last_ping_at >= 8:
                        ws.send("PING")
                        last_ping_at = time.time()
                    try:
                        message = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not message or message == "PONG":
                        continue
                    self._handle_message(message)
            except Exception as exc:
                logger.warning("Polymarket user WebSocket reconnecting after error: %s", exc)
                time.sleep(2)
            finally:
                if self._ws is not None:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

    def _snapshot_condition_ids(self) -> set[str]:
        with self._lock:
            return set(self._condition_ids)

    def _sync_condition_subscription(self, ws: Any, subscribed_ids: set[str], current_ids: set[str]) -> None:
        add_ids = sorted(current_ids - subscribed_ids)
        remove_ids = sorted(subscribed_ids - current_ids)
        if add_ids:
            ws.send(json.dumps({"operation": "subscribe", "markets": add_ids}))
        if remove_ids:
            ws.send(json.dumps({"operation": "unsubscribe", "markets": remove_ids}))

    def _handle_message(self, message: str) -> None:
        payload = json.loads(message)
        events = payload if isinstance(payload, list) else [payload]
        for event in events:
            if not isinstance(event, dict):
                continue
            order_id = event.get("id") or event.get("orderID") or event.get("orderId")
            if order_id:
                with self._lock:
                    self._order_events[str(order_id)] = event


class MusashiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.timeout = (MUSASHI_CONNECT_TIMEOUT_SECONDS, MUSASHI_READ_TIMEOUT_SECONDS)

    def health(self) -> dict[str, Any]:
        response = self.session.get(f"{self.base_url}/api/health", timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_feed(self, limit: int = 20, min_urgency: str = "high") -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/api/feed",
            params={"limit": limit, "minUrgency": min_urgency},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("data", {}).get("tweets", [])

    def analyze_text(self, text: str, min_confidence: float = 0.5, max_results: int = 3) -> dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/api/analyze-text",
            json={"text": text, "minConfidence": min_confidence, "maxResults": max_results},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()


class PolymarketPublicClient:
    def __init__(self) -> None:
        self.session = requests.Session()

    def geoblock(self) -> dict[str, Any]:
        response = self.session.get("https://polymarket.com/api/geoblock", timeout=20)
        response.raise_for_status()
        return response.json()


class PolymarketGammaClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.base_url = "https://gamma-api.polymarket.com"

    def get_market(self, market_id: str) -> dict[str, Any]:
        response = self.session.get(f"{self.base_url}/markets/{market_id}", timeout=20)
        response.raise_for_status()
        return response.json()

    def get_market_by_slug(self, slug: str) -> dict[str, Any]:
        response = self.session.get(f"{self.base_url}/markets/slug/{slug}", timeout=20)
        response.raise_for_status()
        return response.json()

    def search_markets(self, query: str) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/markets",
            params={"search": query, "limit": 10},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return data
        return []

    def resolve_market(self, market: dict[str, Any]) -> dict[str, Any]:
        market_id = market.get("id")
        if market_id:
            try:
                return self.get_market(str(market_id))
            except Exception:
                logger.warning("Gamma lookup by id failed for %s, falling back to slug", market_id)

        slug = extract_slug(market.get("url", ""))
        if not slug:
            candidates = self.search_markets(market.get("title", ""))
            if candidates:
                return candidates[0]
            raise ValueError("Unable to resolve Polymarket slug from market URL")
        try:
            return self.get_market_by_slug(slug)
        except Exception:
            candidates = self.search_markets(market.get("title", ""))
            if candidates:
                return candidates[0]
            raise


def extract_slug(url: str) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    parts = [segment for segment in parsed.path.split("/") if segment]
    if not parts:
        return None
    if "event" in parts:
        idx = parts.index("event")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return parts[-1]


def parse_token_ids(gamma_market: dict[str, Any]) -> list[str]:
    raw = gamma_market.get("clobTokenIds")
    if isinstance(raw, list):
        return [str(item) for item in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except json.JSONDecodeError:
            pass
    return []


def pick_token_id(gamma_market: dict[str, Any], decision_side: str) -> str:
    token_ids = parse_token_ids(gamma_market)
    if len(token_ids) < 2:
        raise ValueError("Gamma market did not return Yes/No token ids")
    return token_ids[0] if decision_side == "YES" else token_ids[1]


def current_probability(market: dict[str, Any], decision_side: str) -> float:
    if decision_side == "YES":
        return float(market.get("yesPrice", 0))
    return float(market.get("noPrice", 0))


@dataclass
class Decision:
    event_id: str
    market: dict[str, Any]
    side: str
    confidence: float
    edge: float
    reason: str
    urgency: str
    signal_type: str | None
    probability: float
    score: float


@dataclass
class FillResult:
    success: bool
    status: str
    order_id: str | None
    filled_shares: float
    filled_value_usd: float
    avg_price: float
    raw_response: dict[str, Any]


@dataclass
class OrderStatusResult:
    order_id: str | None
    status: str
    filled_shares: float
    original_shares: float
    remaining_shares: float
    avg_price: float
    is_open: bool
    is_terminal: bool
    raw_response: dict[str, Any]


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_status(value: Any) -> str:
    return str(value or "unknown").strip().lower().replace("-", "_").replace(" ", "_")


def parse_fill_result(
    response: dict[str, Any],
    fallback_price: float,
    requested_value_usd: float,
    requested_shares: float,
) -> FillResult:
    making_amount = abs(as_float(response.get("makingAmount")))
    taking_amount = abs(as_float(response.get("takingAmount")))

    filled_shares = max(making_amount, taking_amount)
    filled_value_usd = min(making_amount, taking_amount)

    if filled_shares <= 0 and requested_shares > 0 and response.get("success") is True:
        filled_shares = requested_shares
    if filled_value_usd <= 0 and requested_value_usd > 0 and response.get("success") is True:
        filled_value_usd = requested_value_usd

    avg_price = fallback_price
    if filled_shares > 0 and filled_value_usd > 0:
        avg_price = clamp_price(filled_value_usd / filled_shares)

    return FillResult(
        success=bool(response.get("success", True)) and filled_shares > 0,
        status=normalize_status(response.get("status")),
        order_id=response.get("orderID") or response.get("orderId") or response.get("id"),
        filled_shares=round(filled_shares, 6),
        filled_value_usd=round(filled_value_usd, 6),
        avg_price=avg_price,
        raw_response=response,
    )


def parse_order_status(response: dict[str, Any], fallback_price: float) -> OrderStatusResult:
    order_id = response.get("id") or response.get("orderID") or response.get("orderId")
    status = normalize_status(response.get("status"))
    original_shares = as_float(
        response.get("original_size"),
        as_float(response.get("size"), as_float(response.get("initialSize"))),
    )
    filled_shares = as_float(
        response.get("size_matched"),
        as_float(response.get("filledSize"), as_float(response.get("matchedAmount"))),
    )
    remaining_shares = as_float(
        response.get("remaining_size"),
        max(original_shares - filled_shares, 0.0),
    )
    avg_price = clamp_price(
        as_float(response.get("avg_price"), as_float(response.get("price"), fallback_price))
    )
    is_terminal = status in {"filled", "cancelled", "canceled", "expired", "rejected"}
    is_open = status in {"live", "open", "partially_filled", "partially_matched", "matched"} and remaining_shares > 0
    return OrderStatusResult(
        order_id=order_id,
        status=status,
        filled_shares=round(filled_shares, 6),
        original_shares=round(original_shares, 6),
        remaining_shares=round(max(remaining_shares, 0.0), 6),
        avg_price=avg_price,
        is_open=is_open,
        is_terminal=is_terminal,
        raw_response=response,
    )


class PaperTrader:
    def place_market_buy(self, token_id: str, amount_usd: float, meta: dict[str, Any]) -> dict[str, Any]:
        probability = clamp_price(as_float(meta.get("probability"), 0.5))
        shares = estimate_shares(amount_usd, probability)
        logger.info(
            "[paper] buy token=%s amount=%.2f side=%s market=%s",
            token_id,
            amount_usd,
            meta["side"],
            meta["market_title"],
        )
        return {
            "mode": "paper",
            "success": True,
            "status": "filled",
            "token_id": token_id,
            "orderID": f"paper-buy-{int(time.time() * 1000)}",
            "amount_usd": amount_usd,
            "makingAmount": shares,
            "takingAmount": amount_usd,
        }

    def close_position(self, token_id: str, shares: float, limit_price: float, meta: dict[str, Any]) -> dict[str, Any]:
        filled_shares = round(min(shares, as_float(meta.get("available_shares"), shares)), 6)
        proceeds = round(filled_shares * limit_price, 6)
        logger.info(
            "[paper] close token=%s shares=%.4f price=%.4f reason=%s market=%s",
            token_id,
            filled_shares,
            limit_price,
            meta["exit_reason"],
            meta["market_title"],
        )
        return {
            "mode": "paper",
            "success": True,
            "status": "filled",
            "token_id": token_id,
            "shares": filled_shares,
            "price": limit_price,
            "exit_reason": meta["exit_reason"],
            "orderID": f"paper-sell-{int(time.time() * 1000)}",
            "makingAmount": filled_shares,
            "takingAmount": proceeds,
        }

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        return {
            "id": order_id,
            "status": "filled",
            "size_matched": 0,
            "remaining_size": 0,
        }

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        return {"success": True, "canceled": [order_id]}


class LiveTrader:
    def __init__(self) -> None:
        if not POLYMARKET_PRIVATE_KEY:
            raise ValueError("Missing POLYMARKET_PRIVATE_KEY")
        if not POLYMARKET_FUNDER:
            raise ValueError("Missing POLYMARKET_FUNDER")

        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderType

        self._order_type = OrderType
        self._host = POLYMARKET_HOST
        self._client = ClobClient(
            self._host,
            key=POLYMARKET_PRIVATE_KEY,
            chain_id=POLYMARKET_CHAIN_ID,
            signature_type=POLYMARKET_SIGNATURE_TYPE,
            funder=POLYMARKET_FUNDER,
        )
        self._api_creds = self._client.create_or_derive_api_creds()
        self._client.set_api_creds(self._api_creds)

    def ws_auth_payload(self) -> dict[str, str]:
        return {
            "api_key": self._api_creds.api_key,
            "secret": self._api_creds.api_secret,
            "passphrase": self._api_creds.api_passphrase,
        }

    def get_exit_price(self, token_id: str, fallback_probability: float) -> float:
        try:
            quoted = self._client.get_price(token_id, side="SELL")
            if quoted is not None:
                return clamp_price(float(quoted))
        except Exception as exc:
            logger.warning("Failed to fetch SELL quote for %s: %s", token_id, exc)
        return clamp_price(fallback_probability)

    def place_market_buy(self, token_id: str, amount_usd: float, meta: dict[str, Any]) -> dict[str, Any]:
        from py_clob_client.clob_types import MarketOrderArgs
        from py_clob_client.order_builder.constants import BUY

        market_order = MarketOrderArgs(
            token_id=token_id,
            amount=float(round(amount_usd, 2)),
            side=BUY,
            order_type=self._order_type.FOK,
        )
        signed = self._client.create_market_order(market_order)
        response = self._client.post_order(signed, self._order_type.FOK)
        logger.info(
            "[live] buy token=%s amount=%.2f side=%s market=%s response=%s",
            token_id,
            amount_usd,
            meta["side"],
            meta["market_title"],
            response,
        )
        return response

    def close_position(self, token_id: str, shares: float, limit_price: float, meta: dict[str, Any]) -> dict[str, Any]:
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import SELL

        order = OrderArgs(
            token_id=token_id,
            price=clamp_price(limit_price),
            size=round(max(shares, 0.0), 6),
            side=SELL,
        )
        signed = self._client.create_order(order)
        response = self._client.post_order(signed, self._order_type.GTC)
        logger.info(
            "[live] close token=%s shares=%.4f price=%.4f reason=%s market=%s response=%s",
            token_id,
            shares,
            limit_price,
            meta["exit_reason"],
            meta["market_title"],
            response,
        )
        return response

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        if hasattr(self._client, "get_order"):
            return self._client.get_order(order_id)
        if hasattr(self._client, "get_orders"):
            response = self._client.get_orders({"ids": [order_id]})
            if isinstance(response, list) and response:
                return response[0]
            if isinstance(response, dict):
                data = response.get("data")
                if isinstance(data, list) and data:
                    return data[0]
        raise AttributeError("ClobClient does not expose get_order/get_orders")

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        if hasattr(self._client, "cancel"):
            return self._client.cancel(order_id)
        if hasattr(self._client, "cancel_orders"):
            return self._client.cancel_orders([order_id])
        raise AttributeError("ClobClient does not expose cancel/cancel_orders")


def clamp_price(value: float) -> float:
    return round(min(max(value, 0.01), 0.99), 4)


def estimate_shares(size_usd: float, probability: float) -> float:
    if probability <= 0:
        return 0.0
    return round(size_usd / probability, 6)


def realized_pnl_usd(entry_probability: float, current_probability: float, shares: float) -> float:
    return round((current_probability - entry_probability) * shares, 4)


def current_position_shares(position: dict[str, Any]) -> float:
    return as_float(position.get("shares"), as_float(position.get("estimated_shares")))


class Bot:
    def __init__(self) -> None:
        self.musashi = MusashiClient(BASE_URL)
        self.polymarket_public = PolymarketPublicClient()
        self.geolocation = GeolocationClient()
        self.gamma = PolymarketGammaClient()
        self.positions = load_json(POSITIONS_FILE, {})
        self.seen_event_ids = set(load_json(SEEN_FILE, []))
        self.pending_orders = load_json(PENDING_ORDERS_FILE, {})
        self.trader = LiveTrader() if BOT_MODE == "live" else PaperTrader()
        self.market_stream = PolymarketMarketStream(enabled=POLYMARKET_WS_ENABLED)
        self.user_stream = (
            PolymarketUserStream(self.trader.ws_auth_payload(), enabled=POLYMARKET_WS_ENABLED)
            if isinstance(self.trader, LiveTrader)
            else None
        )
        self.startup_geo_profile: dict[str, str] | None = None

    def current_exposure(self) -> float:
        return sum(float(position.get("size_usd", 0)) for position in self.positions.values())

    def bankroll_remaining(self) -> float:
        remaining = BANKROLL_USD - self.current_exposure()
        return max(0.0, round(remaining, 2))

    def save_state(self) -> None:
        save_json(POSITIONS_FILE, self.positions)
        save_json(PENDING_ORDERS_FILE, self.pending_orders)
        self.sync_realtime_subscriptions()

    def _log_geo(self, prefix: str, location: dict[str, Any]) -> None:
        logger.info(
            "%s ip=%s city=%s region=%s country=%s loc=%s timezone=%s provider=%s",
            prefix,
            location.get("ip"),
            location.get("city"),
            location.get("region"),
            location.get("country"),
            location.get("loc"),
            location.get("timezone"),
            location.get("provider"),
        )

    def _normalize_geo_profile(self, location: dict[str, Any]) -> dict[str, str]:
        return {
            "ip": str(location.get("ip") or "").strip(),
            "country": str(location.get("country") or "").strip().upper(),
            "region": str(location.get("region") or "").strip().upper(),
        }

    def _assert_location_profile_allowed(self, location: dict[str, Any], context: str) -> dict[str, str]:
        profile = self._normalize_geo_profile(location)
        country = profile["country"]
        region = profile["region"]

        if not profile["ip"] or not country:
            raise SafetyShutdown(f"{context}: geolocation incomplete; refusing to continue")
        if country in RESTRICTED_COUNTRIES:
            raise SafetyShutdown(f"{context}: restricted country detected ({country}); terminating")
        if region and region in RESTRICTED_REGIONS.get(country, set()):
            raise SafetyShutdown(f"{context}: restricted region detected ({country}-{region}); terminating")
        return profile

    def assert_runtime_safety(self, context: str, *, log_checks: bool = False) -> None:
        location = self.geolocation.locate()
        if not location:
            raise SafetyShutdown(f"{context}: geolocation unavailable; refusing to continue")
        if log_checks:
            self._log_geo("Runtime geolocation:", location)
        current_profile = self._assert_location_profile_allowed(location, context)

        if self.startup_geo_profile is None:
            self.startup_geo_profile = current_profile
        else:
            for key in ("ip", "country", "region"):
                previous = self.startup_geo_profile.get(key, "")
                current = current_profile.get(key, "")
                if previous and current and previous != current:
                    raise SafetyShutdown(
                        f"{context}: detected {key} change from {previous} to {current}; terminating"
                    )

        geo = self.polymarket_public.geoblock()
        if log_checks:
            logger.info("Polymarket geoblock: %s", geo)
        if bool(geo.get("blocked")):
            raise SafetyShutdown(f"{context}: Polymarket geoblock reports blocked=true; terminating")

        geo_country = str(geo.get("country") or "").strip().upper()
        geo_region = str(geo.get("region") or "").strip().upper()
        geo_ip = str(geo.get("ip") or "").strip()
        if geo_country in RESTRICTED_COUNTRIES:
            raise SafetyShutdown(f"{context}: Polymarket geoblock country is restricted ({geo_country}); terminating")
        if geo_region and geo_region in RESTRICTED_REGIONS.get(geo_country, set()):
            raise SafetyShutdown(f"{context}: Polymarket geoblock region is restricted ({geo_country}-{geo_region}); terminating")
        if self.startup_geo_profile:
            startup_ip = self.startup_geo_profile.get("ip", "")
            if startup_ip and geo_ip and geo_ip != startup_ip:
                raise SafetyShutdown(f"{context}: geoblock IP changed from {startup_ip} to {geo_ip}; terminating")

    def sync_realtime_subscriptions(self) -> None:
        asset_ids = {
            str(item.get("token_id"))
            for item in [*self.positions.values(), *self.pending_orders.values()]
            if item.get("token_id")
        }
        self.market_stream.set_assets(asset_ids)

        if self.user_stream:
            condition_ids = {
                str(item.get("condition_id"))
                for item in [*self.positions.values(), *self.pending_orders.values()]
                if item.get("condition_id")
            }
            self.user_stream.set_markets(condition_ids)

    def start_realtime_streams(self) -> None:
        self.sync_realtime_subscriptions()
        self.market_stream.start()
        if self.user_stream:
            self.user_stream.start()

    def current_quote_probability(self, token_id: str) -> float | None:
        quote = self.market_stream.get_quote(token_id)
        if not quote:
            return None
        reference_price = as_float(quote.get("reference_price"))
        if reference_price <= 0:
            return None
        return clamp_price(reference_price)

    def current_exit_quote(self, token_id: str) -> float | None:
        quote = self.market_stream.get_quote(token_id)
        if not quote:
            return None
        for key in ("best_bid", "mid_price", "last_trade_price", "reference_price"):
            price = as_float(quote.get(key))
            if price > 0:
                return clamp_price(price)
        return None

    def latest_position_probability(self, position: dict[str, Any]) -> float:
        token_id = str(position.get("token_id", ""))
        quote_probability = self.current_quote_probability(token_id)
        if quote_probability is not None:
            return quote_probability
        market = self.latest_market_snapshot(position)
        return self.current_position_probability(position, market)

    def has_realtime_pending_updates(self) -> bool:
        if not self.user_stream:
            return False
        return any(self.user_stream.has_update_for_order(order_id) for order_id in self.pending_orders)

    def idle_until_next_scan(self, started_at: float) -> None:
        deadline = started_at + SCAN_INTERVAL_SECONDS
        while time.time() < deadline:
            if self.has_realtime_pending_updates():
                try:
                    self.process_pending_orders()
                except SafetyShutdown:
                    raise
                except Exception as exc:
                    logger.exception("Realtime pending order monitor error: %s", exc)
            time.sleep(1)

    @staticmethod
    def response_indicates_ban_risk(response: dict[str, Any]) -> bool:
        if not isinstance(response, dict):
            return False
        text = json.dumps(response).lower()
        risk_markers = (
            "geoblock",
            "blocked",
            "forbidden",
            "restricted",
            "compliance",
            "sanction",
            "location",
            "region",
            "country",
            "not allowed",
            "not eligible",
            "prohibited",
        )
        return any(marker in text for marker in risk_markers)

    def pending_exit_order_for_market(self, market_id: str) -> tuple[str, dict[str, Any]] | None:
        for order_id, pending in self.pending_orders.items():
            if str(pending.get("market_id")) == str(market_id):
                return order_id, pending
        return None

    def assert_live_trading_allowed(self) -> None:
        self.assert_runtime_safety("startup", log_checks=True)

    def should_trade(self, signal_payload: dict[str, Any]) -> Decision | None:
        if not signal_payload.get("success"):
            return None

        action = signal_payload.get("data", {}).get("suggested_action")
        matches = signal_payload.get("data", {}).get("markets", [])
        urgency = signal_payload.get("urgency")
        event_id = signal_payload.get("event_id")

        if not action or not matches or not event_id:
            return None
        if urgency not in {"high", "critical"}:
            return None
        if action.get("direction") not in {"YES", "NO"}:
            return None
        if float(action.get("confidence", 0)) < MIN_CONFIDENCE:
            return None
        if float(action.get("edge", 0)) < MIN_EDGE:
            return None

        ranked_markets = []
        for match in matches:
            market = match.get("market", {})
            if market.get("platform") != "polymarket":
                continue
            probability = current_probability(market, action["direction"])
            if float(market.get("volume24h", 0)) < MIN_VOLUME_24H:
                continue
            if probability <= MIN_PRICE or probability >= MAX_PRICE:
                continue
            score = float(action["confidence"]) * float(action["edge"]) * max(float(match.get("confidence", 0.5)), 0.25)
            ranked_markets.append((score, market))

        if not ranked_markets:
            return None

        ranked_markets.sort(key=lambda item: item[0], reverse=True)
        score, best_market = ranked_markets[0]
        return Decision(
            event_id=str(event_id),
            market=best_market,
            side=str(action["direction"]),
            confidence=float(action["confidence"]),
            edge=float(action["edge"]),
            reason=str(action.get("reasoning", "")),
            urgency=str(urgency),
            signal_type=signal_payload.get("signal_type"),
            probability=current_probability(best_market, str(action["direction"])),
            score=score,
        )

    def size_position(self, decision: Decision) -> float:
        remaining = min(self.bankroll_remaining(), MAX_TOTAL_EXPOSURE_USD - self.current_exposure())
        if remaining <= 0:
            return 0.0

        if decision.confidence >= 0.88 and decision.edge >= 0.10:
            size = min(MAX_POSITION_USD, 4.0)
        elif decision.confidence >= 0.82 and decision.edge >= 0.07:
            size = min(MAX_POSITION_USD, 3.0)
        else:
            size = min(MAX_POSITION_USD, 2.0)

        return round(max(0.0, min(size, remaining)), 2)

    def record_seen(self, event_id: str) -> None:
        self.seen_event_ids.add(event_id)
        save_json(SEEN_FILE, sorted(self.seen_event_ids))

    def apply_exit_fill_to_position(
        self,
        market_id: str,
        position: dict[str, Any],
        sold_shares: float,
        execution_price: float,
        exit_reason: str,
        response: dict[str, Any],
        order_status: str,
        order_id: str | None,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        current_shares = current_position_shares(position)
        sold_shares = min(max(sold_shares, 0.0), current_shares)
        remaining_shares = round(max(current_shares - sold_shares, 0.0), 6)
        pnl_usd = realized_pnl_usd(float(position["entry_probability"]), execution_price, sold_shares)

        closed_event = {
            **position,
            "closed_at": utc_now_iso(),
            "exit_reason": exit_reason,
            "exit_probability": execution_price,
            "sold_shares": sold_shares,
            "remaining_shares": remaining_shares,
            "realized_pnl_usd_estimate": pnl_usd,
            "close_order_id": order_id,
            "close_order_status": order_status,
            "close_response": response,
        }

        updated_position = None
        if remaining_shares <= 0:
            self.positions.pop(market_id, None)
        else:
            updated_position = {
                **position,
                "shares": remaining_shares,
                "estimated_shares": remaining_shares,
                "size_usd": round(remaining_shares * float(position["entry_probability"]), 6),
                "last_response": response,
                "realized_pnl_usd": round(as_float(position.get("realized_pnl_usd")) + pnl_usd, 6),
            }
            self.positions[market_id] = updated_position

        return closed_event, updated_position

    def already_holding(self, market_id: str, event_id: str) -> bool:
        if market_id in self.positions:
            return True
        if not LIMIT_ONE_POSITION_PER_EVENT:
            return False
        return any(position.get("event_id") == event_id for position in self.positions.values())

    def execute_trade(self, decision: Decision) -> None:
        self.assert_runtime_safety("pre-entry")
        market = decision.market
        market_id = str(market["id"])

        if self.already_holding(market_id, decision.event_id):
            logger.info("Skipped %s because position already exists for market/event", market_id)
            return

        size_usd = self.size_position(decision)
        if size_usd <= 0:
            logger.info("Skipped %s because bankroll/exposure cap is full", market_id)
            return

        gamma_market = self.gamma.resolve_market(market)
        token_id = pick_token_id(gamma_market, decision.side)
        condition_id = extract_condition_id(gamma_market) or extract_condition_id(market)
        response = self.trader.place_market_buy(
            token_id=token_id,
            amount_usd=size_usd,
            meta={
                "side": decision.side,
                "market_title": market["title"],
                "probability": decision.probability,
            },
        )
        if self.response_indicates_ban_risk(response):
            raise SafetyShutdown(f"pre-entry: Polymarket response indicates compliance/geoblock risk; terminating: {response}")
        fill = parse_fill_result(
            response=response,
            fallback_price=decision.probability,
            requested_value_usd=size_usd,
            requested_shares=estimate_shares(size_usd, decision.probability),
        )
        if not fill.success:
            logger.warning("Entry order for %s returned no fill: %s", market_id, response)
            append_jsonl(
                TRADES_FILE,
                {
                    "timestamp": utc_now_iso(),
                    "type": f"{BOT_MODE}_entry_rejected",
                    "market_id": market_id,
                    "token_id": token_id,
                    "response": response,
                    "reason": decision.reason,
                },
            )
            return

        position = {
            "event_id": decision.event_id,
            "market_id": market_id,
            "title": market["title"],
            "url": market.get("url"),
            "side": decision.side,
            "token_id": token_id,
            "condition_id": condition_id,
            "size_usd": fill.filled_value_usd,
            "shares": fill.filled_shares,
            "estimated_shares": fill.filled_shares,
            "entry_probability": fill.avg_price,
            "confidence": decision.confidence,
            "edge": decision.edge,
            "score": decision.score,
            "opened_at": utc_now_iso(),
            "mode": BOT_MODE,
            "entry_order_id": fill.order_id,
            "entry_order_status": fill.status,
            "last_response": fill.raw_response,
            "realized_pnl_usd": 0.0,
        }
        self.positions[market_id] = position
        self.save_state()

        append_jsonl(
            TRADES_FILE,
            {
                "timestamp": utc_now_iso(),
                "type": f"{BOT_MODE}_entry",
                "position": position,
                "reason": decision.reason,
                "signal_type": decision.signal_type,
                "urgency": decision.urgency,
                "fill": fill.raw_response,
            },
        )

    def current_position_probability(self, position: dict[str, Any], market: dict[str, Any]) -> float:
        side = str(position.get("side", "YES"))
        return current_probability(market, side)

    def latest_market_snapshot(self, position: dict[str, Any]) -> dict[str, Any]:
        stub_market = {
            "id": position.get("market_id"),
            "url": position.get("url"),
            "title": position.get("title"),
        }
        return self.gamma.resolve_market(stub_market)

    def should_exit_position(self, position: dict[str, Any], market: dict[str, Any]) -> tuple[str | None, float]:
        current_prob = self.current_position_probability(position, market)
        entry_prob = float(position.get("entry_probability", 0))
        if entry_prob <= 0:
            return "invalid_entry_probability", current_prob

        opened_at = parse_iso_datetime(str(position["opened_at"]))
        hold_minutes = (datetime.now(timezone.utc) - opened_at).total_seconds() / 60
        if current_prob >= entry_prob * (1 + TAKE_PROFIT_PCT):
            return "take_profit", current_prob
        if current_prob <= entry_prob * (1 - STOP_LOSS_PCT):
            return "stop_loss", current_prob
        if MAX_HOLD_MINUTES > 0 and hold_minutes >= MAX_HOLD_MINUTES:
            return "max_hold", current_prob
        return None, current_prob

    def reversed_signal_detected(self, position: dict[str, Any]) -> bool:
        if not EXIT_ON_SIGNAL_REVERSAL:
            return False
        title = str(position.get("title", "")).strip()
        if not title:
            return False
        try:
            signal = self.musashi.analyze_text(title, min_confidence=0.5, max_results=3)
        except Exception as exc:
            logger.warning("Failed to analyze reversal for %s: %s", position.get("market_id"), exc)
            return False
        action = signal.get("data", {}).get("suggested_action") or {}
        new_side = action.get("direction")
        confidence = float(action.get("confidence", 0))
        if new_side not in {"YES", "NO"}:
            return False
        return new_side != position.get("side") and confidence >= MIN_CONFIDENCE

    def close_position(self, market_id: str, position: dict[str, Any], exit_reason: str, current_prob: float) -> None:
        self.assert_runtime_safety("pre-exit")
        existing_pending = self.pending_exit_order_for_market(market_id)
        if existing_pending:
            pending_order_id, pending = existing_pending
            logger.info(
                "Skipped new exit for %s because pending exit order %s already exists with %.4f shares remaining",
                market_id,
                pending_order_id,
                as_float(pending.get("remaining_shares")),
            )
            return

        shares = current_position_shares(position)
        if shares <= 0:
            logger.warning("Skipped close for %s because shares is missing", market_id)
            return

        limit_price = clamp_price(current_prob)
        ws_exit_price = self.current_exit_quote(str(position.get("token_id", "")))
        if ws_exit_price is not None:
            limit_price = ws_exit_price
        if isinstance(self.trader, LiveTrader):
            limit_price = self.trader.get_exit_price(str(position["token_id"]), current_prob)

        response = self.trader.close_position(
            token_id=str(position["token_id"]),
            shares=shares,
            limit_price=limit_price,
            meta={
                "market_title": position["title"],
                "exit_reason": exit_reason,
                "available_shares": shares,
            },
        )
        if self.response_indicates_ban_risk(response):
            raise SafetyShutdown(f"pre-exit: Polymarket response indicates compliance/geoblock risk; terminating: {response}")
        fill = parse_fill_result(
            response=response,
            fallback_price=limit_price,
            requested_value_usd=round(shares * limit_price, 6),
            requested_shares=shares,
        )
        if not fill.success:
            logger.warning("Exit order for %s returned no fill: %s", market_id, response)
            append_jsonl(
                TRADES_FILE,
                {
                    "timestamp": utc_now_iso(),
                    "type": f"{BOT_MODE}_exit_rejected",
                    "market_id": market_id,
                    "token_id": position["token_id"],
                    "response": response,
                    "exit_reason": exit_reason,
                },
            )
            return

        closed, updated_position = self.apply_exit_fill_to_position(
            market_id=market_id,
            position=position,
            sold_shares=fill.filled_shares,
            execution_price=fill.avg_price,
            exit_reason=exit_reason,
            response=fill.raw_response,
            order_status=fill.status,
            order_id=fill.order_id,
        )

        if updated_position and fill.order_id and fill.status in {"live", "open", "matched", "partially_filled", "partially_matched"}:
            self.pending_orders[fill.order_id] = {
                "market_id": market_id,
                "token_id": position["token_id"],
                "condition_id": position.get("condition_id"),
                "side": "SELL",
                "exit_reason": exit_reason,
                "order_id": fill.order_id,
                "created_at": utc_now_iso(),
                "last_checked_at": utc_now_iso(),
                "initial_shares": shares,
                "remaining_shares": current_position_shares(updated_position),
                "limit_price": limit_price,
            }

        self.save_state()
        append_jsonl(
            TRADES_FILE,
            {
                "timestamp": utc_now_iso(),
                "type": f"{BOT_MODE}_exit",
                "position": closed,
                "fill": fill.raw_response,
            },
        )

    def monitor_positions(self) -> None:
        for market_id, position in list(self.positions.items()):
            try:
                if self.pending_exit_order_for_market(market_id):
                    continue
                current_prob = self.latest_position_probability(position)
                exit_reason, current_prob = self.should_exit_position(
                    position,
                    {"yesPrice": current_prob, "noPrice": current_prob},
                )
                if not exit_reason and self.reversed_signal_detected(position):
                    exit_reason = "signal_reversal"
                if exit_reason:
                    self.close_position(market_id, position, exit_reason, current_prob)
            except Exception as exc:
                logger.exception("Position monitor error for %s: %s", market_id, exc)

    def process_pending_orders(self) -> None:
        if not self.pending_orders:
            return

        now = datetime.now(timezone.utc)
        for order_id, pending in list(self.pending_orders.items()):
            market_id = str(pending.get("market_id"))
            position = self.positions.get(market_id)
            if not position:
                self.pending_orders.pop(order_id, None)
                continue

            try:
                self.assert_runtime_safety(f"pending-order:{order_id}")
                if self.user_stream:
                    self.user_stream.pop_order_event(order_id)
                status_response = self.trader.get_order_status(order_id)
                if self.response_indicates_ban_risk(status_response):
                    raise SafetyShutdown(
                        f"pending-order:{order_id}: Polymarket response indicates compliance/geoblock risk; terminating: {status_response}"
                    )
                fallback_price = as_float(pending.get("limit_price"), as_float(position.get("entry_probability"), 0.5))
                status = parse_order_status(status_response, fallback_price=fallback_price)
                pending["last_checked_at"] = utc_now_iso()

                previously_remaining = as_float(pending.get("remaining_shares"), current_position_shares(position))
                newly_filled = round(max(previously_remaining - status.remaining_shares, 0.0), 6)

                if newly_filled > 0:
                    closed, updated_position = self.apply_exit_fill_to_position(
                        market_id=market_id,
                        position=position,
                        sold_shares=newly_filled,
                        execution_price=status.avg_price,
                        exit_reason=str(pending.get("exit_reason", "pending_exit_fill")),
                        response=status.raw_response,
                        order_status=status.status,
                        order_id=status.order_id,
                    )
                    append_jsonl(
                        TRADES_FILE,
                        {
                            "timestamp": utc_now_iso(),
                            "type": f"{BOT_MODE}_exit_fill_update",
                            "position": closed,
                            "fill": status.raw_response,
                        },
                    )
                    position = updated_position

                if status.remaining_shares <= 0 or status.is_terminal and status.status in {"filled", "cancelled", "canceled", "expired"}:
                    self.pending_orders.pop(order_id, None)
                    self.save_state()
                    continue

                pending["remaining_shares"] = status.remaining_shares
                created_at = parse_iso_datetime(str(pending["created_at"]))
                age_seconds = (now - created_at).total_seconds()

                if EXIT_ORDER_REPRICE and age_seconds >= EXIT_ORDER_TIMEOUT_SECONDS:
                    cancel_response = self.trader.cancel_order(order_id)
                    append_jsonl(
                        TRADES_FILE,
                        {
                            "timestamp": utc_now_iso(),
                            "type": f"{BOT_MODE}_exit_cancel_reprice",
                            "market_id": market_id,
                            "order_id": order_id,
                            "cancel_response": cancel_response,
                        },
                    )
                    self.pending_orders.pop(order_id, None)

                    refreshed_position = self.positions.get(market_id)
                    if not refreshed_position:
                        self.save_state()
                        continue

                    latest_market = self.latest_market_snapshot(refreshed_position)
                    current_prob = self.current_position_probability(refreshed_position, latest_market)
                    self.close_position(
                        market_id=market_id,
                        position=refreshed_position,
                        exit_reason=f"{pending.get('exit_reason', 'pending_exit')}_reprice",
                        current_prob=current_prob,
                    )
                    self.save_state()
                    continue

                self.pending_orders[order_id] = pending
                self.save_state()
            except SafetyShutdown:
                raise
            except Exception as exc:
                logger.exception("Pending order monitor error for %s: %s", order_id, exc)

    def reconcile_startup_state(self) -> None:
        self.assert_live_trading_allowed()

        if not STARTUP_RECONCILE:
            return

        logger.info(
            "Startup reconcile: %d positions, %d pending orders",
            len(self.positions),
            len(self.pending_orders),
        )

        changed = False

        for order_id, pending in list(self.pending_orders.items()):
            market_id = str(pending.get("market_id"))
            position = self.positions.get(market_id)

            if not position:
                logger.warning("Removing zombie pending order %s because market %s has no local position", order_id, market_id)
                self.pending_orders.pop(order_id, None)
                changed = True
                continue

            try:
                self.assert_runtime_safety(f"startup-reconcile:{order_id}")
                status_response = self.trader.get_order_status(order_id)
                if self.response_indicates_ban_risk(status_response):
                    raise SafetyShutdown(
                        f"startup-reconcile:{order_id}: Polymarket response indicates compliance/geoblock risk; terminating: {status_response}"
                    )
                fallback_price = as_float(pending.get("limit_price"), as_float(position.get("entry_probability"), 0.5))
                status = parse_order_status(status_response, fallback_price=fallback_price)
            except SafetyShutdown:
                raise
            except Exception as exc:
                logger.warning("Could not reconcile pending order %s: %s", order_id, exc)
                continue

            previously_remaining = as_float(pending.get("remaining_shares"), current_position_shares(position))
            newly_filled = round(max(previously_remaining - status.remaining_shares, 0.0), 6)

            if newly_filled > 0:
                closed, updated_position = self.apply_exit_fill_to_position(
                    market_id=market_id,
                    position=position,
                    sold_shares=newly_filled,
                    execution_price=status.avg_price,
                    exit_reason=f"{pending.get('exit_reason', 'startup_reconcile')}_startup_reconcile",
                    response=status.raw_response,
                    order_status=status.status,
                    order_id=status.order_id,
                )
                append_jsonl(
                    TRADES_FILE,
                    {
                        "timestamp": utc_now_iso(),
                        "type": f"{BOT_MODE}_startup_reconcile_fill",
                        "position": closed,
                        "fill": status.raw_response,
                    },
                )
                position = updated_position
                changed = True

            refreshed_position = self.positions.get(market_id)
            if not refreshed_position:
                self.pending_orders.pop(order_id, None)
                changed = True
                continue

            if status.remaining_shares <= 0 or status.is_terminal:
                logger.info("Removing terminal pending order %s with status=%s", order_id, status.status)
                self.pending_orders.pop(order_id, None)
                changed = True
                continue

            self.pending_orders[order_id]["remaining_shares"] = status.remaining_shares
            self.pending_orders[order_id]["last_checked_at"] = utc_now_iso()
            changed = True

        pending_market_ids = {str(pending.get("market_id")) for pending in self.pending_orders.values()}
        for market_id, position in list(self.positions.items()):
            shares = current_position_shares(position)
            if shares <= 0:
                logger.warning("Removing inconsistent local position %s because remaining shares <= 0", market_id)
                self.positions.pop(market_id, None)
                changed = True
                continue

            if market_id in pending_market_ids:
                continue

        if changed:
            self.save_state()

    def handle_feed_item(self, item: dict[str, Any]) -> None:
        event_id = item.get("event_id")
        if not event_id or event_id in self.seen_event_ids:
            return

        tweet = item.get("tweet", {})
        tweet_text = tweet.get("text", "")
        if not tweet_text or not tweet_text.strip():
            self.record_seen(str(event_id))
            return

        signal = self.musashi.analyze_text(tweet_text, min_confidence=0.5, max_results=3)
        self.record_seen(str(event_id))
        decision = self.should_trade(signal)
        if not decision:
            return
        self.execute_trade(decision)

    def run(self) -> None:
        try:
            try:
                health = self.musashi.health()
                logger.info("Musashi health: %s", bool(health.get("success")))
            except Exception as exc:
                logger.warning("Musashi health check failed: %s", exc)
            logger.info(
                "Bot mode=%s bankroll=%.2f max_position=%.2f exposure_cap=%.2f",
                BOT_MODE,
                BANKROLL_USD,
                MAX_POSITION_USD,
                MAX_TOTAL_EXPOSURE_USD,
            )
            self.start_realtime_streams()
            self.reconcile_startup_state()

            while True:
                loop_started_at = time.time()
                try:
                    self.process_pending_orders()
                    self.monitor_positions()
                    feed = self.musashi.get_feed(limit=20, min_urgency="high")
                    logger.info("Fetched %d feed items", len(feed))
                    for item in feed:
                        self.handle_feed_item(item)
                except SafetyShutdown:
                    raise
                except requests_exceptions.ConnectTimeout:
                    logger.warning(
                        "Musashi feed connect timeout after %.1fs; retrying next cycle",
                        MUSASHI_CONNECT_TIMEOUT_SECONDS,
                    )
                except requests_exceptions.ReadTimeout:
                    logger.warning(
                        "Musashi feed read timeout after %.1fs (connect timeout %.1fs); retrying next cycle",
                        MUSASHI_READ_TIMEOUT_SECONDS,
                        MUSASHI_CONNECT_TIMEOUT_SECONDS,
                    )
                except Exception as exc:
                    logger.exception("Loop error: %s", exc)
                self.idle_until_next_scan(loop_started_at)
        except SafetyShutdown as exc:
            logger.critical("Safety shutdown: %s", exc)
            raise SystemExit(1) from exc
        finally:
            self.market_stream.stop()
            if self.user_stream:
                self.user_stream.stop()


if __name__ == "__main__":
    Bot().run()
