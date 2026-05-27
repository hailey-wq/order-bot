from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
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
            "mode": "BOARD!T21",   # <- 여기만 실제 mode 셀로 바꾸기
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
            "mode": None,
            "buy": "BOARD!B6:C100",
            "sell": "BOARD!E6:F100",
            "moc_buy": None,
            "moc_sell": None,
        },
    },
]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
NUMERIC_CLEANER = re.compile(r"[^0-9.\-]")


@dataclass
class SheetOrders:
    source_name: str
    trade_date: str
    buy_orders: List[Dict[str, Any]]
    sell_orders: List[Dict[str, Any]]
    moc_buy_qty: int = 0
    moc_sell_qty: int = 0
    source_mode: str = ""


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
    source_mode = cell_to_scalar(get_rows(ranges_cfg.get("mode")), default="").strip().upper()

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
        source_mode=source_mode,
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
# 구글시트
# =========================

OUTPUT_SPREADSHEET_ID = "1Pt7k3F5lTMfwQfvDW7VA3MBGFQjigkwmInYxFLTVpZE"
OUTPUT_SHEET_NAME = "Order"


def get_output_account() -> str:
    """Order 탭 J열에 쓸 계좌번호를 .env에서 읽는다."""
    account = (
        os.environ.get("KB_ORDER_ACCOUNT", "").strip()
        or os.environ.get("ORDER_ACCOUNT", "").strip()
    )
    account = re.sub(r"\D", "", account)
    if not account:
        raise RuntimeError(
            "KB_ORDER_ACCOUNT env var is missing. "
            "Add KB_ORDER_ACCOUNT=계좌번호 to your .env file."
        )
    return account


def build_sheet_order_rows(
    buy_orders: List[Dict[str, Any]],
    sell_orders: List[Dict[str, Any]],
    account: str,
    moc_buy_qty: int = 0,
    moc_sell_qty: int = 0,
) -> List[List[Any]]:
    """
    구글시트 Order 탭 I:O 출력용 행 생성.

    I: 주문 여부 TRUE
    J: 계좌번호
    K: 종목
    L: 매수/매도
    M: TWAP/VWAP/LOC/MOC
    N: 가격
    O: 주문량
    """
    rows: List[List[Any]] = []

    def make_row(side: str, method: str, price_text: str, qty: int) -> List[Any]:
        return [True, account, "SOXL", side, method, price_text, int(qty)]

    def add_split_rows(
        side: str,
        orders: List[Dict[str, Any]],
        first_label: str,
        second_label: str = "LOC",
    ) -> None:
        normalized = sorted(
            [{"price": float(x["price"]), "qty": int(x["qty"])} for x in orders],
            key=lambda x: x["price"],
        )

        first_rows: List[List[Any]] = []
        second_rows: List[List[Any]] = []

        for order in normalized:
            first_qty, second_qty = split_qty_front_heavy(order["qty"])
            price_text = f'{order["price"]:.2f}'

            if first_qty > 0:
                first_rows.append(make_row(side, first_label, price_text, first_qty))
            if second_qty > 0:
                second_rows.append(make_row(side, second_label, price_text, second_qty))

        rows.extend(first_rows)
        rows.extend(second_rows)

    # 매수: TWAP 먼저, LOC 나중. 홀수는 TWAP에 1주 더 배정.
    add_split_rows("매수", buy_orders, first_label="TWAP", second_label="LOC")

    if int(moc_buy_qty or 0) > 0:
        rows.append(make_row("매수", "MOC", "", int(moc_buy_qty)))

    # 매도: VWAP 먼저, LOC 나중. 홀수는 VWAP에 1주 더 배정.
    add_split_rows("매도", sell_orders, first_label="VWAP", second_label="LOC")

    if int(moc_sell_qty or 0) > 0:
        rows.append(make_row("매도", "MOC", "", int(moc_sell_qty)))

    return rows


def clear_and_write_order_sheet(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    buy_orders: List[Dict[str, Any]],
    sell_orders: List[Dict[str, Any]],
    account: str,
    moc_buy_qty: int = 0,
    moc_sell_qty: int = 0,
):
    rows = build_sheet_order_rows(
        buy_orders=buy_orders,
        sell_orders=sell_orders,
        account=account,
        moc_buy_qty=moc_buy_qty,
        moc_sell_qty=moc_sell_qty,
    )

    # I:O 전체를 지운 뒤, I4부터 새 주문표를 한 번에 입력.
    # I/J까지 매번 갱신해야 과거 TRUE/계좌번호가 남아 중복 주문되는 위험을 줄일 수 있음.
    service.spreadsheets().values().batchClear(
        spreadsheetId=spreadsheet_id,
        body={"ranges": [f"{sheet_name}!I4:O1000"]},
    ).execute()

    if rows:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!I4:O{3 + len(rows)}",
            valueInputOption="USER_ENTERED",
            body={"values": rows},
        ).execute()


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
        lines.append(f"{float(order['price']):.2f} × {int(order['qty']):,}")
    if int(moc_qty or 0) > 0:
        lines.append(f"MOC × {int(moc_qty):,}")
    if not lines:
        lines.append("-")
    return lines


def split_qty_front_heavy(qty: int) -> Tuple[int, int]:
    """
    홀수 수량은 앞쪽 주문방식에 1주를 더 배정.

    예시:
    - 20 -> 10 / 10
    - 41 -> 21 / 20
    - 255 -> 128 / 127
    """
    qty = int(qty or 0)
    first_qty = (qty + 1) // 2
    second_qty = qty // 2
    return first_qty, second_qty


def build_split_order_lines(
    orders: List[Dict[str, Any]],
    first_label: str,
    second_label: str = "LOC",
) -> List[str]:
    """
    텔레그램 표시용 주문방식 분리.

    매수: first_label="TWAP", second_label="LOC"
    매도: first_label="VWAP", second_label="LOC"
    """
    sorted_orders = sorted(
        [{"price": float(x["price"]), "qty": int(x["qty"])} for x in orders],
        key=lambda x: x["price"],
    )

    first_lines: List[str] = []
    second_lines: List[str] = []

    for order in sorted_orders:
        first_qty, second_qty = split_qty_front_heavy(order["qty"])
        price_text = f'{order["price"]:.2f}'

        if first_qty > 0:
            first_lines.append(f"{first_label} {price_text} × {first_qty:,}")
        if second_qty > 0:
            second_lines.append(f"{second_label} {price_text} × {second_qty:,}")

    lines = first_lines + second_lines
    if not lines:
        lines.append("-")
    return lines


def calculate_order_total(orders: List[Dict[str, Any]]) -> Decimal:
    """가격 × 수량 합계. MOC처럼 가격이 없는 주문은 포함하지 않음."""
    total = Decimal("0")
    for order in orders:
        price = Decimal(str(order["price"]))
        qty = Decimal(int(order["qty"]))
        total += price * qty
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def format_dollar_amount(amount: Decimal) -> str:
    amount_text = f"{amount:,.2f}"
    if "." in amount_text:
        amount_text = amount_text.rstrip("0").rstrip(".")
    return f"${amount_text}"


def build_range_order_lines(
    buy_orders: List[Dict[str, Any]],
    sell_orders: List[Dict[str, Any]],
) -> List[str]:
    buy_orders = sorted(
        [{"price": float(x["price"]), "qty": int(x["qty"])} for x in buy_orders],
        key=lambda x: x["price"],
    )
    sell_orders = sorted(
        [{"price": float(x["price"]), "qty": int(x["qty"])} for x in sell_orders],
        key=lambda x: x["price"],
    )

    lines: List[str] = []

    # 매수 구간: 낮을수록 더 많이 매수
    if buy_orders:
        remaining_buy = sum(x["qty"] for x in buy_orders)
        first_price = buy_orders[0]["price"]
        lines.append(f"{first_price:.2f} 이하 : {remaining_buy:,} 매수")

        for i in range(1, len(buy_orders)):
            remaining_buy -= buy_orders[i - 1]["qty"]
            lower = round(buy_orders[i - 1]["price"] + 0.01, 2)
            upper = buy_orders[i]["price"]
            lines.append(f"{lower:.2f} ~ {upper:.2f} : {remaining_buy:,} 매수")

    # 매수/매도 사이 한 줄 띄우기
    if buy_orders and sell_orders:
        lines.append("")

    # 매도 구간: 높을수록 더 많이 매도
    if sell_orders:
        cumulative_sell = 0
        for i, order in enumerate(sell_orders):
            cumulative_sell += order["qty"]
            price = order["price"]

            if i < len(sell_orders) - 1:
                upper = round(sell_orders[i + 1]["price"] - 0.01, 2)
                lines.append(f"{price:.2f} ~ {upper:.2f} : {cumulative_sell:,} 매도")
            else:
                lines.append(f"{price:.2f} 이상 : {cumulative_sell:,} 매도")

    return lines


def format_source_display_name(item: SheetOrders) -> str:
    mode = (item.source_mode or "").strip().upper()
    if item.source_name == "YJ" and mode in {"SP", "SH"}:
        return f"{item.source_name} ({mode})"
    return item.source_name



def build_message(inputs: List[SheetOrders], optimized: Dict[str, Any]) -> str:
    dates = [item.trade_date for item in inputs if item.trade_date]
    date_line = dates[0] if dates else "날짜 없음"
    all_same_date = len(set(dates)) <= 1 if dates else True

    buy_orders = sorted(
        optimized.get("buy_orders", []),
        key=lambda x: float(x["price"]),
    )
    sell_orders = sorted(
        optimized.get("sell_orders", []),
        key=lambda x: float(x["price"]),
    )

    moc_buy_qty = int(optimized.get("moc_buy_qty", 0) or 0)
    moc_sell_qty = int(optimized.get("moc_sell_qty", 0) or 0)

    lines = [
        "통합 주문표",
        str(date_line),
        "",
        "📌 매수",
    ]

    # 매수는 TWAP 먼저, LOC 나중. 홀수는 TWAP에 1주 더 배정.
    lines.extend(build_split_order_lines(buy_orders, first_label="TWAP", second_label="LOC"))

    # 매수 총액은 실제 통합 매수 주문의 가격 × 수량 합계.
    if buy_orders:
        buy_total = calculate_order_total(buy_orders)
        lines.extend(["", f"매수 총 {format_dollar_amount(buy_total)}"])

    lines.extend(["", "📌 매도"])

    # 매도는 VWAP 먼저, LOC 나중. 홀수는 VWAP에 1주 더 배정.
    lines.extend(build_split_order_lines(sell_orders, first_label="VWAP", second_label="LOC"))

    lines.append("")

    for item in inputs:
        buy_text = f"buy {len(item.buy_orders)}건"
        if item.moc_buy_qty:
            buy_text += f" + MOC {item.moc_buy_qty:,}"

        sell_text = f"sell {len(item.sell_orders)}건"
        if item.moc_sell_qty:
            sell_text += f" + MOC {item.moc_sell_qty:,}"

        display_name = format_source_display_name(item)
        lines.append(f"{display_name} | {buy_text} | {sell_text}")

    # 구간별 1회 주문은 실제 통합 주문 수량 기준으로 그대로 유지.
    range_lines = build_range_order_lines(buy_orders, sell_orders)
    if range_lines:
        lines.extend(["", "📊 구간별 1회 주문"])
        lines.extend(range_lines)

    # MOC는 여기서만 따로 표시
    if moc_sell_qty > 0:
        lines.extend(["", "🕘 종가정리", f"MOC 매도 {moc_sell_qty:,}주"])

    # 필요하면 나중에 예외 처리용으로 살릴 수 있음
    # if moc_buy_qty > 0:
    #     lines.extend(["", f"⚠️ 예외: MOC 매수 {moc_buy_qty:,}주"])

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

    output_account = get_output_account()

    clear_and_write_order_sheet(
        service=service,
        spreadsheet_id=OUTPUT_SPREADSHEET_ID,
        sheet_name=OUTPUT_SHEET_NAME,
        buy_orders=optimized.get("buy_orders", []),
        sell_orders=optimized.get("sell_orders", []),
        account=output_account,
        moc_buy_qty=optimized.get("moc_buy_qty", 0),
        moc_sell_qty=optimized.get("moc_sell_qty", 0),
    )

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

