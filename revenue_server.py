# revenue_server.py
import requests, json, os, logging, base64, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import xml.etree.ElementTree as ET
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import fcntl
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === КОНФИГИ ===
AUTH_URL = "https://chi-fan-co.iiko.it/resto/api/auth"
REPORT_URL = "https://chi-fan-co.iiko.it/resto/api/reports/olap"
REPORT_V2_URL = "https://chi-fan-co.iiko.it/resto/api/v2/reports/olap"
LOGIN = "User"
PASSWORD = "9ab5284fa9b0a51a61a3b59189c04f9e4779720a"

RENAME_MAP = {
    "Чифань": "Чуркин", "Сибирцева": "Красота", "Вынос Светланская": "Светланская",
    "Чи-фань Тихая": "Тихая", "Нагорный Парк": "Парк", "Новоивановская": "Луговая",
    "Набережная": "Юбилейный", "Кунгасный": "Кунгас", "МГУ": "МГУ", "Патрокл": "Патрокл",
    "Тобольская": "Тобольская"
}

YEAR_ROUND = ["Красота", "Луговая", "Парк", "Светланская", "Чуркин", "Тихая", "Тобольская"]
SEASONAL = ["Юбилейный", "Кунгас", "МГУ", "Патрокл"]

DISHES = ["Лапша с курицей и овощами","Лапша с курицей","Рис с курицей и овощами","Азиатский салат с курицей",
          "Лапша со свининой","Лапша со свининой и овощами","Рис с беконом и кимчи","Сливочная лапша с беконом",
          "Рис с креветкой и овощами","Тайский суп с креветкой","Азиатский салат с креветкой","Стеклянная лапша с креветкой",
          "Лапша с креветкой и овощами","Лапша с креветкой","Сливочная лапша с креветкой","Рис с овощами, грибами и яйцом",
          "Рис","Рис(гарнир)","Лапша с овощами, грибами и фуджу","Стеклянная лапша с креветкой и тофу",
          "+ Бекон 70г","+ Креветки 70г","+ Курица 70г","+ Свинина 70г","Азиатский салат",
          "Вьетнамский куриный суп с лапшой и яйцом","Вьетнамский суп с лапшой","Кола-комбо «Мясной»",
          "Тайский суп с курицей","Пельмени с курицей","Бао-банс с курицей"]

CATEGORIES = { "Лапша": ["Лапша с курицей и овощами","Лапша с курицей","Лапша со свининой","Лапша со свининой и овощами",
                        "Сливочная лапша с беконом","Лапша с креветкой и овощами","Лапша с креветкой","Сливочная лапша с креветкой",
                        "Лапша с овощами, грибами и фуджу"],
               "Рис": ["Рис с курицей и овощами","Рис с беконом и кимчи","Рис с креветкой и овощами",
                       "Рис с овощами, грибами и яйцом","Рис","Рис(гарнир)"],
               "Курица": ["Рис с курицей и овощами","Лапша с курицей и овощами","Лапша с курицей","Азиатский салат с курицей",
                          "+ Курица 70г","Пельмени с курицей","Бао-банс с курицей"],
               "Свинина": ["Лапша со свининой","Лапша со свининой и овощами","+ Свинина 70г"],
               "Креветка": ["Лапша с креветкой","Стеклянная лапша с креветкой","Азиатский салат с креветкой",
                            "Рис с креветкой и овощами","Тайский суп с креветкой","Лапша с креветкой и овощами",
                            "Сливочная лапша с креветкой","Стеклянная лапша с креветкой и тофу","+ Креветки 70г"],
               "Бекон": ["Рис с беконом и кимчи","Сливочная лапша с беконом","+ Бекон 70г"],
               "Соломка": ["Лапша с курицей и овощами","Лапша с курицей","Лапша со свининой","Лапша со свининой и овощами",
                           "Лапша с креветкой и овощами","Лапша с креветкой","Лапша с овощами, грибами и фуджу"],
               "Кубик": ["Рис с курицей и овощами","Рис с креветкой и овощами","Рис с овощами, грибами и яйцом"],
               "Салаты - Супы": ["Азиатский салат","Азиатский салат с креветкой","Азиатский салат с курицей",
                                 "Вьетнамский куриный суп с лапшой и яйцом","Вьетнамский суп с лапшой","Кола-комбо «Мясной»",
                                 "Тайский суп с креветкой","Тайский суп с курицей"],
               "Пельмени - Бао-банс": ["Пельмени с курицей","Бао-банс с курицей"]
}

DISH_MAPPING = {
    "Азиатский салат": "Азиатский салат с курицей",
    "Вьетнамский суп с лапшой": "Вьетнамский куриный суп с лапшой и яйцом",
    "Вьетнамский куриный суп с лапшой и яйцом": "Вьетнамский куриный суп с лапшой и яйцом",
    "Кола-комбо «Мясной»": "Азиатский салат с курицей"
}

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
        return r.text.strip()
    except Exception as e:
        logger.error(f"Токен не получен: {e}")
        return None

def fetch_sales(date_from, date_to):
    token = get_token()
    if not token: return None
    params = {
        "key": token, "report": "SALES",
        "from": f"{date_from}T00:00:00", "to": f"{date_to}T23:59:59",
        "groupRow": ["Delivery.ServiceType","DeletedWithWriteoff","RestorauntGroup"],
        "agr": ["DishDiscountSumInt","UniqOrderId"]
    }
    try:
        r = session.get(REPORT_URL, params=params, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        data = []
        for row in root.findall('r'):
            if row.find('DeletedWithWriteoff').text == "DELETED": continue
            venue = RENAME_MAP.get(row.find('RestorauntGroup').text, row.find('RestorauntGroup').text)
            service = row.find('Delivery.ServiceType').text or ""
            orders = int(row.find('UniqOrderId').text or 0)
            revenue = float(row.find('DishDiscountSumInt').text or 0)
            data.append({"venue": venue, "service_type": service, "orders": orders, "revenue": revenue})
        return data
    except Exception as e:
        logger.error(f"Ошибка продаж: {e}")
        return None

def fetch_dishes(date_from, date_to):
    token = get_token()
    if not token: return None
    params = {
        "key": token, "report": "SALES",
        "from": f"{date_from}T00:00:00", "to": f"{date_to}T23:59:59",
        "groupRow": ["DishFullName","RestorauntGroup"], "agr": ["DishAmountInt"]
    }
    try:
        r = session.post(REPORT_V2_URL, json=params, timeout=20)
        r.raise_for_status()
        result = []
        for row in r.json().get("data", []):
            dish = row.get("DishFullName")
            if dish in DISHES:
                venue = RENAME_MAP.get(row.get("RestorauntGroup"), row.get("RestorauntGroup"))
                amount = float(row.get("DishAmountInt", 0))
                result.append({"dish": dish, "venue": venue, "amount": amount})
        return result
    except Exception as e:
        logger.error(f"Ошибка блюд: {e}")
        return None

def aggregate_sales(raw):
    yr = {v: {"total_orders":0,"total_revenue":0,"delivery_orders":0,"delivery_revenue":0,
                      "pickup_orders":0,"pickup_revenue":0,"cafe_orders":0,"cafe_revenue":0} for v in YEAR_ROUND}
    sn = {v: {"total_orders":0,"total_revenue":0,"delivery_orders":0,"delivery_revenue":0,
                      "pickup_orders":0,"pickup_revenue":0,"cafe_orders":0,"cafe_revenue":0} for v in SEASONAL}
    for r in raw or []:
        if r["venue"] in YEAR_ROUND: stats = yr
        elif r["venue"] in SEASONAL: stats = sn
        else: continue
        s = stats[r["venue"]]
        s["total_orders"] += r["orders"]
        s["total_revenue"] += r["revenue"]
        if r["service_type"] == "Доставка":
            s["delivery_orders"] += r["orders"]; s["delivery_revenue"] += r["revenue"]
        elif r["service_type"] == "Самовывоз":
            s["pickup_orders"] += r["orders"]; s["pickup_revenue"] += r["revenue"]
        else:
            s["cafe_orders"] += r["orders"]; s["cafe_revenue"] += r["revenue"]
    return yr, sn

def aggregate_pf(dishes):
    sales = {v: {} for v in YEAR_ROUND + SEASONAL}
    for r in dishes or []:
        venue = r["venue"]
        if venue not in sales: continue
        dish = DISH_MAPPING.get(r["dish"], r["dish"])
        for cat, list_dishes in CATEGORIES.items():
            if dish in list_dishes:
                if cat in ["Салаты - Супы", "Пельмени - Бао-банс"]:
                    if cat not in sales[venue]: sales[venue][cat] = {}
                    if dish not in sales[venue][cat]: sales[venue][cat][dish] = 0
                    sales[venue][cat][dish] += r["amount"]
                else:
                    if cat not in sales[venue]: sales[venue][cat] = 0
                    if cat in ["Соломка"]: sales[venue][cat] += r["amount"] * 0.10
                    elif cat in ["Кубик"]: sales[venue][cat] += r["amount"] * 0.11
                    else: sales[venue][cat] += r["amount"]
    return sales

def calc_plan():
    now = datetime.now()
    if plan_cache["month"] != now.strftime("%Y-%m"):
        start = (now - relativedelta(months=1)).replace(day=1).strftime("%Y-%m-%d")
        end = (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
        prev = fetch_sales(start, end)
        if prev:
            yr, sn = aggregate_sales(prev)
            total_rev = sum(s["total_revenue"] for s in list(yr.values()) + list(sn.values()))
            total_ord = sum(s["total_orders"] for s in list(yr.values()) + list(sn.values()))
            plan_cache["Общее"] = {"revenue": round(total_rev * PLAN_COEFFICIENT),
                                   "orders": round(total_ord * PLAN_COEFFICIENT)}
            plan_cache["month"] = now.strftime("%Y-%m")

def get_business_day():
    now = datetime.now()
    if now.hour < 3: now -= timedelta(days=1)
    return now.strftime("%Y-%m-%d")

def main():
    calc_plan()
    date_str = get_business_day()
    time_str = datetime.now().strftime("%H:%M")

    sales_raw = fetch_sales(date_str, date_str)
    dishes_raw = fetch_dishes(date_str, date_str)

    yr, sn = aggregate_sales(sales_raw)
    pf = aggregate_pf(dishes_raw)

    totals = {"total_orders":0,"total_revenue":0,"total_delivery_orders":0,"total_delivery_revenue":0,
             "total_pickup_orders":0,"total_pickup_revenue":0,"total_cafe_orders":0,"total_cafe_revenue":0}
    for s in list(yr.values()) + list(sn.values()):
        for k in totals: totals[k] += s[k]

    output = {
        "date": date_str,
        "time": time_str,
        "year_round": yr,
        "seasonal": sn,
        "total": totals,
        "plan": plan_cache["Общее"],
        "sales": pf
    }

    # Запись текущего
    with open("data/revenue.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Архивация завершённого дня (03:00–03:10)
    now = datetime.now()
    if 3 <= now.hour < 4 and now.minute <= 10:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        arch_output = output.copy()
        arch_output["date"] = yesterday
        arch_output["time"] = "23:59"
        path = f"data/daily/{yesterday}_data.json"
        os.makedirs("data/daily", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(arch_output, f, ensure_ascii=False, indent=2)
        logger.info(f"Архивирован день {yesterday}")

    logger.info(f"Обновлено {date_str} {time_str} → {totals['total_revenue']:,.0f} ₽")

if __name__ == "__main__":
    main()