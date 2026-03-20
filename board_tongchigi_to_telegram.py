from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# =========================
# FIXED USER CONFIG
# =========================
SOURCES = [
    {
        "name": "YJ",
        "spreadsheet_id": "1_LdL5U_zIcVG1bv8MJmVgKsOZq75i-zeNOJioREiT-4",
        "ranges": {
            "date": "BOARD!B2",
            "buy": "BOARD!B6:C100",
            "sell": "BOARD!E6:F100",
            "moc_buy": None,
            "moc_sell": None,
        },
    },
    {
        "name": "FBRS",
        "spreadsheet_id": "1Edgcu4-T6aKG1jiNdKU3GMnzsToxKmJawsO8YGdVFTo",
        "ranges": {
            "date": "BOARD!B2",
            "buy": "BOARD!B6:C100",
            "sell": "BOARD!E6:F100",
            "moc_buy": None,
            "moc_sell": None,
        },
    },
]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
NUMERIC_CLEANER = re.compile(r"[^0-9.\-]")


@dataclass
class SheetOrders:
    source_name: str
    trade_date: str
    buy_orders: List[Dict[str, Any]]
    sell_orders: List[Dict[str, Any]]
    moc_buy_qty: int = 0
    moc_sell_qty: int = 0


# =========================
# OPTIONAL .env LOADER
# =========================
def load_dotenv_if_present(dotenv_path: str = ".env") -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


# =========================
# CORE TUNGGCHIGI LOGIC
# =========================
def optimize_orders(
    buy_orders: List[Dict[str, float]],
    sell_orders: List[Dict[str, float]],
    moc_buy_qty: int = 0,
    moc_sell_qty: int = 0,
) -> Dict[str, Any]:
    """
    MOC 포함 통합 퉁치기.

    핵심 아이디어:
    - MOC BUY는 매우 높은 가격의 매수로 간주
    - MOC SELL은 매우 낮은 가격의 매도로 간주
    - 그 상태에서 기존 퉁치기 로직을 그대로 적용

    예시:
    - 매도 MOC 300 + 매수 54.13 x 200
      -> 매도 54.14 x 200 + 매도 MOC 100

    - 매도 MOC 200 + 매수 54.13 x 300
      -> 매수 54.13 x 100 + 매도 54.14 x 200
    """
    MOC_BUY_PRICE = 999999.0
    MOC_SELL_PRICE = 0.01

    price_levels: Dict[float, Dict[str, int]] = {}

    def add_price(price: float, qty: int, is_buy: bool) -> None:
        if price not in price_levels:
            price_levels[price] = {"qty": 0, "buy_qty": 0, "sell_qty": 0}
        price_levels[price]["qty"] += qty
        if is_buy:
            price_levels[price]["buy_qty"] += qty
        else:
            price_levels[price]["sell_qty"] += qty

    total_buy_qty = int(moc_buy_qty or 0)

    if int(moc_buy_qty or 0) > 0:
        add_price(MOC_BUY_PRICE, int(moc_buy_qty), True)
    if int(moc_sell_qty or 0) > 0:
        add_price(MOC_SELL_PRICE, int(moc_sell_qty), False)

    for order in buy_orders:
        add_price(float(order["price"]), int(order["qty"]), True)
        total_buy_qty += int(order["qty"])

    for order in sell_orders:
        add_price(float(order["price"]), int(order["qty"]), False)

    sorted_prices = sorted(price_levels.keys())

    new_buy_orders: List[Dict[str, Any]] = []
    new_sell_orders: List[Dict[str, Any]] = []
    new_moc_buy = 0
    new_moc_sell = 0

    remaining_buy = total_buy_qty

    for price in sorted_prices:
        info = price_levels[price]
        qty_at_price = info["qty"]
        if qty_at_price == 0:
            continue

        buy_alloc = min(remaining_buy, qty_at_price)
        sell_alloc = qty_at_price - buy_alloc
        remaining_buy -= buy_alloc

        if buy_alloc > 0:
            if price == MOC_BUY_PRICE:
                new_moc_buy += buy_alloc
            elif price != MOC_SELL_PRICE:
                qty_from_buy = min(buy_alloc, info["buy_qty"])
                qty_from_sell = buy_alloc - qty_from_buy
                if qty_from_buy > 0:
                    new_buy_orders.append({"price": price, "qty": qty_from_buy})
                if qty_from_sell > 0:
                    new_buy_orders.append(
                        {"price": round(price - 0.01, 2), "qty": qty_from_sell}
                    )

        if sell_alloc > 0:
            if price == MOC_SELL_PRICE:
                new_moc_sell += sell_alloc
            elif price != MOC_BUY_PRICE:
                qty_from_sell = min(sell_alloc, info["sell_qty"])
                qty_from_buy = sell_alloc - qty_from_sell
                if qty_from_sell > 0:
                    new_sell_orders.append({"price": price, "qty": qty_from_sell})
                if qty_from_buy > 0:
                    new_sell_orders.append(
                        {"price": round(price + 0.01, 2), "qty": qty_from_buy}
                    )

    def aggregate_orders(orders: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        aggregated: Dict[float, int] = {}
        for order in orders:
            p = float(order["price"])
            aggregated[p] = aggregated.get(p, 0) + int(order["qty"])
        return [
            {"price": p, "qty": q}
            for p, q in sorted(aggregated.items())
            if q > 0
        ]

    return {
        "buy_orders": aggregate_orders(new_buy_orders),
        "sell_orders": aggregate_orders(new_sell_orders),
        "moc_buy_qty": int(new_moc_buy),
        "moc_sell_qty": int(new_moc_sell),
    }


# =========================
# GOOGLE SHEETS
# =========================
def build_sheets_service():
    if os.environ.get("GOOGLE_JSON", "").strip():
        info = json.loads(os.environ["GOOGLE_JSON"])
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=SCOPES,
        )
    else:
        service_account_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
        if not service_account_file:
            raise RuntimeError(
                "GOOGLE_JSON or GOOGLE_SERVICE_ACCOUNT_FILE env var is missing."
            )
        creds = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=SCOPES,
        )

    return build("sheets", "v4", credentials=creds, cache_discovery=False)



def to_float(value: Any) -> float:
    if value is None:
        raise ValueError("Empty numeric cell")
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = NUMERIC_CLEANER.sub("", str(value))
    if cleaned == "":
        raise ValueError(f"Cannot parse float from {value!r}")
    return float(cleaned)



def to_int(value: Any) -> int:
    return int(round(to_float(value)))



def is_moc_value(value: Any) -> bool:
    return isinstance(value, str) and value.strip().upper() == "MOC"



def parse_order_rows(rows: List[List[Any]]) -> Tuple[List[Dict[str, Any]], int]:
    orders: List[Dict[str, Any]] = []
    moc_qty = 0

    for row in rows:
        if not row or len(row) < 2:
            continue

        price_raw = row[0]
        qty_raw = row[1]

        if price_raw in (None, "") or qty_raw in (None, ""):
            continue

        try:
            qty = to_int(qty_raw)
        except ValueError:
            continue

        if qty <= 0:
            continue

        if is_moc_value(price_raw):
            moc_qty += qty
            continue

        try:
            price = round(to_float(price_raw), 2)
        except ValueError:
            continue

        orders.append({"price": price, "qty": qty})

    return orders, moc_qty



def cell_to_scalar(rows: List[List[Any]], default: str = "") -> str:
    if not rows or not rows[0]:
        return default
    return str(rows[0][0])



def cell_to_int(rows: List[List[Any]], default: int = 0) -> int:
    if not rows or not rows[0]:
        return default
    value = rows[0][0]
    if value in (None, ""):
        return default
    return to_int(value)



def read_source(service, source_cfg: Dict[str, Any]) -> SheetOrders:
    spreadsheet_id = source_cfg["spreadsheet_id"]
    ranges_cfg = source_cfg["ranges"]
    request_ranges = [r for r in ranges_cfg.values() if r]

    result = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=request_ranges,
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
    )

    by_range: Dict[str, List[List[Any]]] = {}
    for value_range in result.get("valueRanges", []):
        requested = value_range.get("range", "")
        by_range[requested] = value_range.get("values", [])

    def get_rows(requested_range: Optional[str]) -> List[List[Any]]:
        if not requested_range:
            return []
        if requested_range in by_range:
            return by_range[requested_range]
        for actual_range, rows in by_range.items():
            if actual_range.endswith(requested_range):
                return rows
        return []

    trade_date = cell_to_scalar(get_rows(ranges_cfg.get("date")), default="")
    buy_orders, inline_moc_buy = parse_order_rows(get_rows(ranges_cfg.get("buy")))
    sell_orders, inline_moc_sell = parse_order_rows(get_rows(ranges_cfg.get("sell")))

    moc_buy_qty = cell_to_int(get_rows(ranges_cfg.get("moc_buy")), default=0) + inline_moc_buy
    moc_sell_qty = cell_to_int(get_rows(ranges_cfg.get("moc_sell")), default=0) + inline_moc_sell

    return SheetOrders(
        source_name=source_cfg["name"],
        trade_date=trade_date,
        buy_orders=buy_orders,
        sell_orders=sell_orders,
        moc_buy_qty=moc_buy_qty,
        moc_sell_qty=moc_sell_qty,
    )


# =========================
# TELEGRAM
# =========================
def split_message(text: str, limit: int = 4000) -> List[str]:
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    current: List[str] = []
    current_len = 0
    for line in text.splitlines(keepends=True):
        if current_len + len(line) > limit and current:
            parts.append("".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line)
    if current:
        parts.append("".join(current))
    return parts



def send_telegram_message(text: str) -> None:
    if os.environ.get("DRY_RUN", "").strip() in {"1", "true", "TRUE", "yes", "YES"}:
        print(text)
        return

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID env var is missing.")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for part in split_message(text):
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": part,
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram sendMessage failed: {payload}")


# =========================
# FORMAT
# =========================
def format_orders_plain(
    orders: List[Dict[str, Any]],
    moc_qty: int = 0,
) -> List[str]:
    lines: List[str] = []
    sorted_orders = sorted(orders, key=lambda x: float(x["price"]))
    for order in sorted_orders:
        lines.append(f"{float(order['price']):.2f} × {int(order['qty'])}")
    if int(moc_qty or 0) > 0:
        lines.append(f"MOC × {int(moc_qty)}")
    if not lines:
        lines.append("-")
    return lines



def build_message(inputs: List[SheetOrders], optimized: Dict[str, Any]) -> str:
    dates = [item.trade_date for item in inputs if item.trade_date]
    date_line = dates[0] if dates else "날짜 없음"
    all_same_date = len(set(dates)) <= 1 if dates else True

    lines = [
        "통합 주문표",
        str(date_line),
        "",
        "📌 매수",
    ]

    lines.extend(
        format_orders_plain(
            optimized.get("buy_orders", []),
            int(optimized.get("moc_buy_qty", 0) or 0),
        )
    )

    lines.extend([
        "",
        "📌 매도",
    ])

    lines.extend(
        format_orders_plain(
            optimized.get("sell_orders", []),
            int(optimized.get("moc_sell_qty", 0) or 0),
        )
    )

    lines.append("")

    for item in inputs:
        buy_text = f"buy {len(item.buy_orders)}건"
        if item.moc_buy_qty:
            buy_text += f" + MOC {item.moc_buy_qty}"

        sell_text = f"sell {len(item.sell_orders)}건"
        if item.moc_sell_qty:
            sell_text += f" + MOC {item.moc_sell_qty}"

        lines.append(f"{item.source_name} | {buy_text} | {sell_text}")

    if not all_same_date:
        lines.extend(["", "⚠️ 시트 날짜가 서로 다름"])

    return "\n".join(lines)


# =========================
# MAIN
# =========================
def main() -> None:
    load_dotenv_if_present()
    service = build_sheets_service()

    inputs: List[SheetOrders] = []
    all_buy_orders: List[Dict[str, Any]] = []
    all_sell_orders: List[Dict[str, Any]] = []
    total_moc_buy = 0
    total_moc_sell = 0

    for source_cfg in SOURCES:
        sheet_orders = read_source(service, source_cfg)
        inputs.append(sheet_orders)
        all_buy_orders.extend(sheet_orders.buy_orders)
        all_sell_orders.extend(sheet_orders.sell_orders)
        total_moc_buy += sheet_orders.moc_buy_qty
        total_moc_sell += sheet_orders.moc_sell_qty

    optimized = optimize_orders(
        buy_orders=all_buy_orders,
        sell_orders=all_sell_orders,
        moc_buy_qty=total_moc_buy,
        moc_sell_qty=total_moc_sell,
    )

    message = build_message(inputs, optimized)
    send_telegram_message(message)

    print(
        json.dumps(
            {
                "status": "ok",
                "sources": [item.source_name for item in inputs],
                "date_candidates": [item.trade_date for item in inputs],
                "final_buy_count": len(optimized.get("buy_orders", [])),
                "final_sell_count": len(optimized.get("sell_orders", [])),
                "final_moc_buy_qty": int(optimized.get("moc_buy_qty", 0) or 0),
                "final_moc_sell_qty": int(optimized.get("moc_sell_qty", 0) or 0),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
