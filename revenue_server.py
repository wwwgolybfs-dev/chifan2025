# revenue_server.py
import requests, json, os, logging, base64, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import xml.etree.ElementTree as ET
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === КОНФИГИ ===
AUTH_URL = "https://chi-fan-co.iiko.it/resto/api/auth"
REPORT_URL = "https://chi-fan-co.iiko.it/resto/api/reports/olap"
REPORT_V2_URL = "https://chi-fan-co.iiko.it/resto/api/v2/reports/olap"
LOGIN = "User"  # Если логин другой — поменяй здесь
import os
PASSWORD = os.getenv("IIKO_PASSWORD")  # Берём из секрета

if not PASSWORD:
    logger.error("IIKO_PASSWORD не найден в секретах!")
    sys.exit(1)

RENAME_MAP = {
    "Чифань": "Чуркин", "Сибирцева": "Красота", "Вынос Светланская": "Светланская",
    "Чи-фань Тихая": "Тихая", "Нагорный Парк": "Парк", "Новоивановская": "Луговая",
    "Набережная": "Юбилейный", "Кунгасный": "Кунгас", "МГУ": "МГУ", "Патрокл": "Патрокл",
    "Тобольская": "Тобольская"
}

YEAR_ROUND = ["Красота", "Луговая", "Парк", "Светланская", "Чуркин", "Тихая", "Тобольская"]
SEASONAL = ["Юбилейный", "Кунгас", "МГУ", "Патрокл"]

# (DISHES, CATEGORIES, DISH_MAPPING — оставь как было в твоём старом коде, они длинные, я не менял)

# ... здесь вставь твои списки DISHES, CATEGORIES, DISH_MAPPING из старого кода ...

PLAN_COEFFICIENT = 0.991
plan_cache = {"Общее": {"revenue": 0, "orders": 0}, "month": None}

session = requests.Session()
session.verify = False
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504,429])
session.mount("https://", HTTPAdapter(max_retries=retries))

def get_token():
    try:
        r = session.post(AUTH_URL, data={"login": LOGIN, "pass": PASSWORD}, timeout=15)
        r.raise_for_status()
        token = r.text.strip('"')  # Иногда токен в кавычках
        logger.info(f"Токен получен: {token[:10]}...")
        return token
    except Exception as e:
        logger.error(f"Токен не получен: {e} | Response: {e.response.text if hasattr(e, 'response') else 'No response'}")
        return None

# fetch_sales, fetch_dishes — оставь как было

def aggregate_sales(raw):
    default_stats = {"total_orders":0,"total_revenue":0,"delivery_orders":0,"delivery_revenue":0,
                     "pickup_orders":0,"pickup_revenue":0,"cafe_orders":0,"cafe_revenue":0}
    yr = {v: default_stats.copy() for v in YEAR_ROUND}
    sn = {v: default_stats.copy() for v in SEASONAL}
    for r in raw or []:
        venue = r["venue"]
        if venue in YEAR_ROUND:
            s = yr[venue]
        elif venue in SEASONAL:
            s = sn[venue]
        else:
            continue
        s["total_orders"] += r["orders"]
        s["total_revenue"] += r["revenue"]
        if r["service_type"] == "Доставка":
            s["delivery_orders"] += r["orders"]
            s["delivery_revenue"] += r["revenue"]
        elif r["service_type"] == "Самовывоз":
            s["pickup_orders"] += r["orders"]
            s["pickup_revenue"] += r["revenue"]
        else:
            s["cafe_orders"] += r["orders"]
            s["cafe_revenue"] += r["revenue"]
    return yr, sn

# aggregate_pf — оставь как было

# calc_plan, get_business_day, main — оставь как было, но в main добавь проверки if sales_raw is None: continue или просто пропуск

def main():
    calc_plan()
    date_str = get_business_day()
    time_str = datetime.now().strftime("%H:%M")

    sales_raw = fetch_sales(date_str, date_str)
    if not sales_raw:
        logger.warning("Нет данных продаж за день")
        sales_raw = []
    dishes_raw = fetch_dishes(date_str, date_str) or []

    yr, sn = aggregate_sales(sales_raw)
    pf = aggregate_pf(dishes_raw)

    totals = {"total_orders":0,"total_revenue":0,"total_delivery_orders":0,"total_delivery_revenue":0,
              "total_pickup_orders":0,"total_pickup_revenue":0,"total_cafe_orders":0,"total_cafe_revenue":0}
    for s in list(yr.values()) + list(sn.values()):
        for k in totals:
            totals[k] += s.get(k, 0)  # Безопасно

    output = {
        "date": date_str,
        "time": time_str,
        "year_round": yr,
        "seasonal": sn,
        "total": totals,
        "plan": plan_cache["Общее"],
        "sales": pf
    }

    os.makedirs("data", exist_ok=True)
    with open("data/revenue.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Архивация (если ночь)
    now = datetime.now()
    if 3 <= now.hour < 4 and now.minute <= 10:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        arch = output.copy()
        arch["date"] = yesterday
        arch["time"] = "23:59"
        os.makedirs("data/daily", exist_ok=True)
        with open(f"data/daily/{yesterday}_data.json", "w", encoding="utf-8") as f:
            json.dump(arch, f, ensure_ascii=False, indent=2)

    logger.info(f"Успех! {date_str} {time_str} — {totals['total_revenue']:,.0f} ₽")

if __name__ == "__main__":
    main()
