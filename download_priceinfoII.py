import os, time, math, requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

# ============================
# CONFIG
# ============================
API_URL = "https://web-api.tp.entsoe.eu/api"
#TOKEN   = os.getenv("ENTSOE_TOKEN") # define por env-var ou cola aqui
TOKEN = '940d85aa-ff19-4efb-8a23-dbf53b9a8c43'
PT_EIC  = "10YPT-REN------W"   # Bidding zone Portugal (ENTSO-E)  # fonte: ENTSO-E docs
DOC     = "A44"                # Energy Prices (Day-ahead), ver Postman collection


# Intervalo: últimos 4 anos (UTC) arredondado à hora
end_utc   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
start_utc = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)

# A API impõe limites — pedimos por janelas de ~31 dias
STEP_DAYS = 31

def fmt(dt): 
    """Formata timestamp no padrão exigido pela API: yyyyMMddHHmm (UTC)."""
    return dt.strftime("%Y%m%d%H%M")

def parse_ack(xml_bytes):
    """Lê Acknowledgement_MarketDocument (erros como 401/4xx) e devolve motivos."""
    try:
        root = ET.fromstring(xml_bytes)
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0"}
        reasons = []
        for r in root.findall(".//ns:Reason", ns):
            code = r.findtext("./ns:code", default="", namespaces=ns)
            text = r.findtext("./ns:text", default="", namespaces=ns)
            reasons.append((code, text))
        return reasons
    except Exception:
        return []

def fetch_prices_window(start_dt_utc, end_dt_utc):
    """Pede preços day-ahead para [start,end) e devolve lista de dicts {datetime_utc, price_eur_mwh}."""
    params = {
        "securityToken": TOKEN,
        "documentType": DOC,
        "in_Domain": PT_EIC,
        "out_Domain": PT_EIC,
        "periodStart": fmt(start_dt_utc),
        "periodEnd":   fmt(end_dt_utc),
    }
    r = requests.get(API_URL, params=params, timeout=60)

    if r.status_code == 401:
        reasons = parse_ack(r.content)
        raise RuntimeError(f"401 Unauthorized; Reasons: {reasons or 'N/A'}")
    if r.status_code != 200:
        # Algumas vezes devolve XML de erro com 409/400 quando a janela é vazia ou demasiado longa
        text = r.text[:300].replace("\n"," ")
        raise RuntimeError(f"HTTP {r.status_code}: {text}")

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        raise RuntimeError(f"XML parse error: {e}")

    # Namespace 451-3 pode variar 7:0, 7:3, 6:0. Tentamos várias assinaturas.
    candidates = [
        {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"},
        {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"},
        {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:6:0"},
    ]
    rows = []
    ok = False
    for cand in candidates:
        ns = {"ns": cand["ns"]}
        ts_list = root.findall(".//ns:TimeSeries", ns)
        if not ts_list:
            continue
        ok = True
        for ts in ts_list:
            period = ts.find(".//ns:Period", ns)
            if period is None:
                continue
            start_text = period.findtext("./ns:timeInterval/ns:start", default=None, namespaces=ns)
            if not start_text:
                continue
            # ISO8601 -> datetime UTC
            t0 = datetime.fromisoformat(start_text.replace("Z", "+00:00"))

            for p in period.findall("./ns:Point", ns):
                pos_txt = p.findtext("./ns:position", "0", namespaces=ns)
                price_txt = p.findtext("./ns:price.amount", None, namespaces=ns)
                if not pos_txt or price_txt is None:
                    continue
                pos = int(pos_txt)
                try:
                    price = float(price_txt)
                except ValueError:
                    continue

                ts_utc = t0 + timedelta(hours=pos - 1)  # resolução horário PT60M
                rows.append({
                    "datetime_utc": ts_utc.isoformat(),
                    "price_eur_mwh": price
                })
        break

    if not ok:
        # Se nada casou, pode ter vindo outro documento (ex.: acknowledgement mesmo com 200)
        raise RuntimeError("Documento inesperado (TimeSeries não encontrado).")

    return rows

# ==============
# LOOP JANELAS
# ==============
all_rows = []
cursor = start_utc
while cursor < end_utc:
    win_start = cursor
    win_end   = min(cursor + timedelta(days=STEP_DAYS), end_utc)
    print(f"Janela: {win_start.date()} -> {win_end.date()}")

    try:
        chunk = fetch_prices_window(win_start, win_end)
        print(f"  + {len(chunk)} registos")
        all_rows.extend(chunk)
    except Exception as e:
        print(f"  ! Falha: {e}")

    # Respeitar gateway
    time.sleep(0.7)
    cursor = win_end

# ============================
# CSVs: horário + média diária
# ============================
if not all_rows:
    raise SystemExit("Nenhum registo obtido. Verifique token, janelas e conectividade.")

df = pd.DataFrame(all_rows).sort_values("datetime_utc")
df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
df.to_csv("entsoe_day_ahead_PT_hourly_4y.csv", index=False)

daily = df.assign(date=df["datetime_utc"].dt.date) \
          .groupby("date", as_index=False)["price_eur_mwh"].mean() \
          .rename(columns={"price_eur_mwh":"avg_price_eur_mwh"})
daily.to_csv("entsoe_day_ahead_PT_dailyavg_4y.csv", index=False)

print("\n📁 Criados:")
print(" - entsoe_day_ahead_PT_hourly_4y.csv   (preço horário, UTC)")
print(" - entsoe_day_ahead_PT_dailyavg_4y.csv (média diária)")