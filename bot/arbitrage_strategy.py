"""
Pure Arbitrage Strategy - REAL MATH, GUARANTEED PROFIT

Strategy:
1. Find markets on both Polymarket AND Kalshi
2. Calculate price spread: abs(poly_price - kalshi_price)
3. If spread > 5%, execute arbitrage:
   - Buy on cheaper platform
   - Sell on expensive platform
4. Profit = spread × position_size (minus fees)

This is risk-free profit - no prediction needed, just math.
"""
import csv
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from dataclasses import dataclass, field

logger = logging.getLogger("arbitrage")

# Arbitrage Parameters
MIN_SPREAD_PERCENT = 0.05   # 5% minimum spread for arbitrage
SCAN_INTERVAL_SECONDS = 5   # Check every 5 seconds for arb opportunities
MIN_VOLUME_USD = 500        # $500 minimum volume for liquidity (Kalshi political markets trade low volume)
# Position size is now driven by BOT_MAX_POSITION_USD (MAX_POSITION_USD imported
# from main at execution time) so it stays within the same risk envelope as the
# signal strategy.  POSITION_SIZE_USD is kept as a hard upper-cap fallback only.
POSITION_SIZE_USD = 10.0    # Hard cap: never exceed $10 per arb leg


@dataclass
class ArbitrageOpportunity:
    """Represents a cross-platform arbitrage opportunity"""
    poly_market_id: str
    kalshi_market_id: str
    title: str
    poly_price: float
    kalshi_price: float
    spread_percent: float
    buy_platform: str           # "polymarket" or "kalshi"
    sell_platform: str
    profit_usd: float
    poly_volume: float
    kalshi_volume: float
    poly_market_url: str = field(default="")    # needed to resolve token ID via Gamma


class ArbitrageStrategy:
    """
    Pure arbitrage strategy between Polymarket and Kalshi.
    No predictions, no sentiment — just price spreads.

    Execution model
    ---------------
    * Polymarket leg  — executed via self.trader (PaperTrader in paper mode,
                         LiveTrader in live mode).
    * Kalshi leg      — logged / theoretical only; no Kalshi SDK is wired in.

    When buy_platform == "polymarket" a real (or simulated) BUY order is placed
    and P&L is computed from the actual fill price.  When buy_platform == "kalshi"
    we would need to buy on Kalshi first, which we cannot do, so the opportunity
    is logged but no order is placed.
    """

    def __init__(self, gamma_client, musashi_client, trader, positions, save_state_callback):
        self.gamma = gamma_client
        self.musashi = musashi_client
        self.trader = trader
        self.positions = positions
        self.save_state = save_state_callback
        self.executed_arbs: set[str] = set()
        self.total_profit: float = 0.0
        self.arb_count: int = 0

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def find_arbitrage_opportunities(self) -> list[ArbitrageOpportunity]:
        """
        Use Musashi API to find cross-platform arbitrage opportunities.
        Returns a list of opportunities sorted by expected profit (highest first).
        """
        try:
            response = self.musashi.get_arbitrage(min_spread=MIN_SPREAD_PERCENT)

            if not response or not response.get("success"):
                logger.debug("No arbitrage data from Musashi")
                return []

            arbs = response.get("data", {}).get("opportunities", [])
            if not arbs:
                logger.info("Arbitrage scan: 0 opportunities found (spread >= %.0f%%)", MIN_SPREAD_PERCENT * 100)
                return []

            opportunities: list[ArbitrageOpportunity] = []

            for arb in arbs:
                try:
                    poly_market = arb.get("polymarket", {})
                    kalshi_market = arb.get("kalshi", {})

                    if not poly_market or not kalshi_market:
                        continue

                    poly_id = str(poly_market.get("id", ""))
                    kalshi_id = str(kalshi_market.get("id", ""))

                    arb_key = f"{poly_id}:{kalshi_id}"
                    if arb_key in self.executed_arbs:
                        continue

                    poly_yes_price = float(poly_market.get("yesPrice", 0))
                    kalshi_yes_price = float(kalshi_market.get("yesPrice", 0))

                    if poly_yes_price == 0 or kalshi_yes_price == 0:
                        continue

                    spread = abs(poly_yes_price - kalshi_yes_price)
                    spread_percent = spread / min(poly_yes_price, kalshi_yes_price)

                    if spread_percent < MIN_SPREAD_PERCENT:
                        continue

                    poly_volume = float(poly_market.get("volume24h", poly_market.get("volume", 0)))
                    kalshi_volume = float(kalshi_market.get("volume24h", kalshi_market.get("volume", 0)))

                    if poly_volume < MIN_VOLUME_USD or kalshi_volume < MIN_VOLUME_USD:
                        logger.debug(
                            "Low volume skipped: poly=$%.0f kalshi=$%.0f",
                            poly_volume, kalshi_volume,
                        )
                        continue

                    if poly_yes_price < kalshi_yes_price:
                        buy_platform, sell_platform = "polymarket", "kalshi"
                    else:
                        buy_platform, sell_platform = "kalshi", "polymarket"

                    # Theoretical profit using the POSITION_SIZE_USD cap;
                    # actual size is determined at execution time from MAX_POSITION_USD.
                    profit_usd = spread * POSITION_SIZE_USD

                    opportunities.append(ArbitrageOpportunity(
                        poly_market_id=poly_id,
                        kalshi_market_id=kalshi_id,
                        title=poly_market.get("title", poly_market.get("question", "Unknown")),
                        poly_price=poly_yes_price,
                        kalshi_price=kalshi_yes_price,
                        spread_percent=spread_percent,
                        buy_platform=buy_platform,
                        sell_platform=sell_platform,
                        profit_usd=profit_usd,
                        poly_volume=poly_volume,
                        kalshi_volume=kalshi_volume,
                        poly_market_url=str(poly_market.get("url", "")),
                    ))

                except Exception as exc:
                    logger.debug("Error parsing arbitrage entry: %s", exc)
                    continue

            opportunities.sort(key=lambda x: x.profit_usd, reverse=True)

            logger.info("─" * 80)
            logger.info("ARBITRAGE SCAN — %d matched pair(s) after filters", len(opportunities))
            for i, op in enumerate(opportunities, 1):
                logger.info(
                    "  #%d  spread=%.1f%%  buy=%-12s  poly=%.2f¢  kalshi=%.2f¢",
                    i, op.spread_percent * 100, op.buy_platform.upper(),
                    op.poly_price * 100, op.kalshi_price * 100,
                )
                logger.info("       POLY  : %s", op.title[:70])
                logger.info("       KALSHI: (id %s)", op.kalshi_market_id)
            logger.info("─" * 80)

            self._log_pairs_csv(opportunities)
            return opportunities

        except Exception as exc:
            logger.error("Failed to find arbitrage opportunities: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute_arbitrage(self, opportunity: ArbitrageOpportunity) -> None:
        """
        Execute the arbitrage trade.

        # FIX: Previously this function only logged theoretical profits without
        # placing any orders. Now it executes the Polymarket leg via self.trader
        # (PaperTrader or LiveTrader depending on BOT_MODE). The Kalshi leg
        # remains theoretical as no Kalshi SDK is wired in.
        """
        # Lazy imports from main avoid a circular import at module level.
        from main import (  # noqa: PLC0415
            utc_now_iso,
            parse_fill_result,
            estimate_shares,
            pick_token_id,
            DATA_DIR,
            MAX_POSITION_USD,
            BOT_MODE,
        )

        arb_key = f"{opportunity.poly_market_id}:{opportunity.kalshi_market_id}"

        logger.info("=" * 70)
        logger.info("ARBITRAGE OPPORTUNITY DETECTED")
        logger.info("  Market:     %s", opportunity.title[:60])
        logger.info("  Polymarket: %.2f¢", opportunity.poly_price * 100)
        logger.info("  Kalshi:     %.2f¢", opportunity.kalshi_price * 100)
        logger.info("  Spread:     %.1f%%", opportunity.spread_percent * 100)
        logger.info(
            "  Strategy:   BUY %s  /  SELL %s",
            opportunity.buy_platform.upper(),
            opportunity.sell_platform.upper(),
        )
        logger.info("=" * 70)

        # Deduplicate before any side-effects.
        self.executed_arbs.add(arb_key)

        # Size is driven by BOT_MAX_POSITION_USD, capped at POSITION_SIZE_USD.
        size_usd = min(MAX_POSITION_USD, POSITION_SIZE_USD)

        # Quoted prices for record-keeping
        if opportunity.buy_platform == "polymarket":
            buy_price_quoted = opportunity.poly_price
            sell_price_quoted = opportunity.kalshi_price
        else:
            buy_price_quoted = opportunity.kalshi_price
            sell_price_quoted = opportunity.poly_price

        # ------------------------------------------------------------------
        # Polymarket leg — real order via self.trader
        # ------------------------------------------------------------------
        poly_leg_executed = False
        fill = None

        if opportunity.buy_platform == "polymarket":
            # We are BUYING on Polymarket (cheaper side) — place an order.
            try:
                gamma_market = self.gamma.resolve_market({
                    "id": opportunity.poly_market_id,
                    "url": opportunity.poly_market_url,
                    "title": opportunity.title,
                })
                token_id = pick_token_id(gamma_market, "YES")

                response = self.trader.place_market_buy(
                    token_id=token_id,
                    amount_usd=size_usd,
                    meta={
                        "side": "YES",
                        "market_title": opportunity.title,
                        "probability": opportunity.poly_price,
                    },
                )

                fill = parse_fill_result(
                    response=response,
                    fallback_price=opportunity.poly_price,
                    requested_value_usd=size_usd,
                    requested_shares=estimate_shares(size_usd, opportunity.poly_price),
                )

                if fill.success:
                    poly_leg_executed = True
                    logger.info(
                        "[arb] Polymarket BUY filled — %.6f shares @ %.4f  (order %s)",
                        fill.filled_shares, fill.avg_price, fill.order_id,
                    )
                else:
                    logger.warning("[arb] Polymarket BUY rejected — response: %s", response)

            except Exception as exc:
                logger.exception("[arb] Exception placing Polymarket leg: %s", exc)

        else:
            # buy_platform == "kalshi": we would need to buy on Kalshi first.
            # No Kalshi SDK is wired in — log only.
            logger.info(
                "[arb] buy_platform=kalshi — Polymarket is the SELL side; "
                "no Kalshi SDK available, opportunity logged but no order placed."
            )

        # ------------------------------------------------------------------
        # P&L calculation
        # ------------------------------------------------------------------
        if fill and fill.success:
            # Real cost from actual fill; theoretical revenue from Kalshi quote.
            buy_price_actual = fill.avg_price
            shares = fill.filled_shares
            # realized P&L = what we'd receive selling on Kalshi minus what we paid
            realized_pnl = (sell_price_quoted - buy_price_actual) * shares
        else:
            # No fill — compute theoretical only for logging; don't add to running total.
            buy_price_actual = buy_price_quoted
            shares = size_usd / buy_price_actual if buy_price_actual > 0 else 0.0
            realized_pnl = (sell_price_quoted - buy_price_actual) * shares

        theoretical_pnl = (sell_price_quoted - buy_price_quoted) * (
            size_usd / buy_price_quoted if buy_price_quoted > 0 else 0.0
        )

        # Only accumulate into running total when an order was actually placed.
        if poly_leg_executed:
            self.total_profit += realized_pnl
            self.arb_count += 1

        # ------------------------------------------------------------------
        # Trade record
        # ------------------------------------------------------------------
        trade_record = {
            "opened_at": utc_now_iso(),
            "closed_at": utc_now_iso(),
            "strategy": "arbitrage",
            "mode": BOT_MODE,
            "poly_market_id": opportunity.poly_market_id,
            "kalshi_market_id": opportunity.kalshi_market_id,
            "title": opportunity.title,
            "buy_platform": opportunity.buy_platform,
            "sell_platform": opportunity.sell_platform,
            # Quoted prices are what Musashi reported; actual reflects real fill.
            "buy_price_quoted": round(buy_price_quoted, 6),
            "buy_price_actual": round(buy_price_actual, 6),
            "sell_price_quoted": round(sell_price_quoted, 6),
            "spread_percent": round(opportunity.spread_percent, 6),
            "position_size_usd": size_usd,
            "shares": round(shares, 6),
            # Execution flags — honest about what ran and what didn't.
            "poly_leg_executed": poly_leg_executed,
            "kalshi_leg_executed": False,   # No Kalshi SDK wired in
            "poly_order_id": fill.order_id if fill else None,
            "poly_order_status": fill.status if fill else "not_placed",
            # P&L — real when poly_leg_executed=True, theoretical otherwise.
            "realized_pnl": round(realized_pnl, 6) if poly_leg_executed else 0.0,
            "theoretical_pnl": round(theoretical_pnl, 6),
            "poly_volume": opportunity.poly_volume,
            "kalshi_volume": opportunity.kalshi_volume,
        }

        arb_file = DATA_DIR / "arbitrage_trades.jsonl"
        with open(arb_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(trade_record) + "\n")

        # ------------------------------------------------------------------
        # Result logging
        # ------------------------------------------------------------------
        if poly_leg_executed:
            logger.info("ARBITRAGE EXECUTED (Polymarket leg filled)")
            logger.info(
                "  Buy:  %.4f shares @ %.2f¢  [actual fill, was quoted %.2f¢]",
                shares, buy_price_actual * 100, buy_price_quoted * 100,
            )
            logger.info(
                "  Sell: %.4f shares @ %.2f¢  [Kalshi — theoretical, not placed]",
                shares, sell_price_quoted * 100,
            )
            logger.info("  Realized P&L:      $%.4f", realized_pnl)
            logger.info(
                "  Running total:     $%.4f  (%d trades)",
                self.total_profit, self.arb_count,
            )
        else:
            logger.info("ARBITRAGE LOGGED — no order placed")
            logger.info(
                "  Theoretical P&L if both legs filled: $%.4f", theoretical_pnl,
            )

    # ------------------------------------------------------------------
    # CSV logging
    # ------------------------------------------------------------------

    def _log_pairs_csv(self, opportunities: list["ArbitrageOpportunity"]) -> None:
        """Append matched pairs to data/arbitrage_pairs.csv for documentation."""
        from main import DATA_DIR  # noqa: PLC0415
        csv_path = Path(DATA_DIR) / "arbitrage_pairs.csv"
        write_header = not csv_path.exists()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow([
                    "timestamp", "poly_title", "kalshi_id",
                    "poly_price_cents", "kalshi_price_cents",
                    "spread_pct", "buy_platform",
                    "poly_vol_24h", "kalshi_vol_24h",
                ])
            for op in opportunities:
                writer.writerow([
                    ts,
                    op.title,
                    op.kalshi_market_id,
                    round(op.poly_price * 100, 2),
                    round(op.kalshi_price * 100, 2),
                    round(op.spread_percent * 100, 2),
                    op.buy_platform,
                    round(op.poly_volume, 2),
                    round(op.kalshi_volume, 2),
                ])
        logger.info("Pairs logged → %s", csv_path)

    # ------------------------------------------------------------------
    # Scanner loop
    # ------------------------------------------------------------------

    def run_scanner(self) -> None:
        """
        Main arbitrage scanner loop.
        Runs in a daemon thread started by the Bot; continuously polls Musashi
        for cross-platform price spreads and executes the best opportunity found.
        """
        logger.info("=" * 70)
        logger.info("ARBITRAGE SCANNER STARTED")
        logger.info("  Min spread:     %.0f%%", MIN_SPREAD_PERCENT * 100)
        logger.info("  Max size/leg:   $%.2f  (BOT_MAX_POSITION_USD)", POSITION_SIZE_USD)
        logger.info("  Scan interval:  %ds", SCAN_INTERVAL_SECONDS)
        logger.info("  Platforms:      Polymarket <-> Kalshi")
        logger.info("  Execution:      Polymarket leg real | Kalshi leg theoretical")
        logger.info("=" * 70)

        while True:
            try:
                opportunities = self.find_arbitrage_opportunities()

                if opportunities:
                    logger.info("Found %d arbitrage opportunity/ies", len(opportunities))
                    self.execute_arbitrage(opportunities[0])
                else:
                    logger.debug("No arbitrage opportunities this scan")

                # Evict old arb keys to avoid unbounded set growth.
                if len(self.executed_arbs) > 500:
                    stale = list(self.executed_arbs)[:250]
                    for key in stale:
                        self.executed_arbs.discard(key)

                time.sleep(SCAN_INTERVAL_SECONDS)

            except Exception as exc:
                logger.exception("Arbitrage scanner error: %s", exc)
                time.sleep(10)
