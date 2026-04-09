#!/usr/bin/env python3
"""
Award Flight Search API
Wraps seats.aero partner API with scoring, trip details, and buy-miles info.

Dev:        python app.py
Production: gunicorn app:app  (Procfile)
"""

import json
import os
import re
import requests as _requests
import time
import urllib.parse
import concurrent.futures

from flask import Flask, request, jsonify

from fast_flights import FlightData, Passengers, get_flights as gf_get_flights

app = Flask(__name__)

SEATS_AERO_KEY = os.environ.get("SEATS_AERO_KEY", "pro_3C3BUt7QiVMzBPN3fwxfU7UCqYc")
BASE_URL        = "https://seats.aero/partnerapi/search"
TRIPS_URL       = "https://seats.aero/partnerapi/trips"

# ── Buy-miles data ─────────────────────────────────────────────────────────────
BUY_MILES_INFO = {
    "aeroplan": {
        "program_name":       "Air Canada Aeroplan",
        "logo_url":           "https://logo.clearbit.com/aeroplan.com",
        "buy_url":            "https://www.aeroplan.com/buy-miles",
        "currency":           "CAD",
        "standard_cpp":       3.50,
        "standard_cpp_usd":   2.43,
        "typical_promo_bonus": 50,
        "promo_cpp_usd":      1.62,
        "min_purchase":       1000,
        "max_purchase":       150000,
        "notes": "Promos (50-100% bonus) run several times per year. Best to wait for one.",
    },
    "alaska": {
        "program_name":       "Alaska Mileage Plan",
        "logo_url":           "https://logo.clearbit.com/alaskaair.com",
        "buy_url":            "https://www.alaskaair.com/content/mileage-plan/ways-to-earn/buy-miles",
        "currency":           "USD",
        "standard_cpp":       2.50,
        "standard_cpp_usd":   2.50,
        "typical_promo_bonus": 60,
        "promo_cpp_usd":      1.56,
        "min_purchase":       1000,
        "max_purchase":       150000,
        "notes": "Frequent 40-100% bonus promos. Never buy at standard rate.",
    },
    "american": {
        "program_name":       "American AAdvantage",
        "logo_url":           "https://logo.clearbit.com/aa.com",
        "buy_url":            "https://www.aa.com/i18n/aadvantage-program/miles/buy-miles.jsp",
        "currency":           "USD",
        "standard_cpp":       2.50,
        "standard_cpp_usd":   2.50,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":      1.79,
        "min_purchase":       1000,
        "max_purchase":       150000,
        "notes": "Promos usually 35-100% bonus. Check aa.com before buying.",
    },
    "virginatlantic": {
        "program_name":       "Virgin Atlantic Flying Club",
        "logo_url":           "https://logo.clearbit.com/virginatlantic.com",
        "buy_url":            "https://www.virginatlantic.com/us/en/flying-club/points/buy-points.html",
        "currency":           "USD",
        "standard_cpp":       2.50,
        "standard_cpp_usd":   2.50,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":      1.79,
        "min_purchase":       500,
        "max_purchase":       100000,
        "notes": "Regular bonus offers. Good for ANA and Delta redemptions.",
    },
    "delta": {
        "program_name":       "Delta SkyMiles",
        "logo_url":           "https://logo.clearbit.com/delta.com",
        "buy_url":            "https://www.delta.com/us/en/skymiles/buy-gift-transfer-miles/buy-miles",
        "currency":           "USD",
        "standard_cpp":       3.50,
        "standard_cpp_usd":   3.50,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":      2.50,
        "min_purchase":       1000,
        "max_purchase":       150000,
        "notes": "Delta miles are dynamic priced — value varies. Only buy for specific high-value routes.",
    },
    "united": {
        "program_name":       "United MileagePlus",
        "logo_url":           "https://logo.clearbit.com/united.com",
        "buy_url":            "https://www.united.com/en/us/fly/mileageplus/miles/buy.html",
        "currency":           "USD",
        "standard_cpp":       3.50,
        "standard_cpp_usd":   3.50,
        "typical_promo_bonus": 100,
        "promo_cpp_usd":      1.75,
        "min_purchase":       1000,
        "max_purchase":       150000,
        "notes": "Occasional 100% bonus sales drop the price significantly.",
    },
    "flyingblue": {
        "program_name":       "Air France/KLM Flying Blue",
        "logo_url":           "https://logo.clearbit.com/flyingblue.com",
        "buy_url":            "https://flyingblue.com/buy-miles",
        "currency":           "EUR",
        "standard_cpp":       2.80,
        "standard_cpp_usd":   2.00,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":      1.43,
        "min_purchase":       2000,
        "max_purchase":       200000,
        "notes": "Monthly Promo Awards offer 25-50% off redemptions — better than buying miles.",
    },
    "etihad": {
        "program_name":       "Etihad Guest",
        "logo_url":           "https://logo.clearbit.com/etihad.com",
        "buy_url":            "https://www.etihad.com/en-us/etihad-guest/miles/buy-miles",
        "currency":           "USD",
        "standard_cpp":       2.80,
        "standard_cpp_usd":   2.80,
        "typical_promo_bonus": 35,
        "promo_cpp_usd":      2.07,
        "min_purchase":       1000,
        "max_purchase":       100000,
        "notes": "Promos offered periodically. Good for Etihad First Apartment redemptions.",
    },
    "singapore": {
        "program_name":       "Singapore KrisFlyer",
        "logo_url":           "https://logo.clearbit.com/singaporeair.com",
        "buy_url":            "https://www.singaporeair.com/en_UK/us/ppsclub-krisflyer/krisflyer/buy-miles/",
        "currency":           "USD",
        "standard_cpp":       2.50,
        "standard_cpp_usd":   2.50,
        "typical_promo_bonus": 30,
        "promo_cpp_usd":      1.92,
        "min_purchase":       1000,
        "max_purchase":       100000,
        "notes": "Good for Singapore Suites (First). Prices vary; watch for promotions.",
    },
    "lufthansa": {
        "program_name":       "Lufthansa Miles & More",
        "logo_url":           "https://logo.clearbit.com/miles-and-more.com",
        "buy_url":            "https://www.miles-and-more.com/row/en/earn/buy-miles.html",
        "currency":           "EUR",
        "standard_cpp":       2.80,
        "standard_cpp_usd":   1.70,
        "typical_promo_bonus": 35,
        "promo_cpp_usd":      1.26,
        "min_purchase":       2000,
        "max_purchase":       100000,
        "notes": "Infrequent promos. Best used for Star Alliance premium cabin sweet spots.",
    },
}

PROGRAM_NAMES = {k: v["program_name"] for k, v in BUY_MILES_INFO.items()}
TAX_FX = {"CAD": 0.694, "EUR": 1.08, "USD": 1.0, "AUD": 0.63, "GBP": 1.27}

CARRIER_LOGOS = {
    "AC": "https://logo.clearbit.com/aircanada.com",
    "LX": "https://logo.clearbit.com/swiss.com",
    "LH": "https://logo.clearbit.com/lufthansa.com",
    "UA": "https://logo.clearbit.com/united.com",
    "AA": "https://logo.clearbit.com/aa.com",
    "DL": "https://logo.clearbit.com/delta.com",
    "AS": "https://logo.clearbit.com/alaskaair.com",
    "BA": "https://logo.clearbit.com/britishairways.com",
    "AF": "https://logo.clearbit.com/airfrance.com",
    "KL": "https://logo.clearbit.com/klm.com",
    "VS": "https://logo.clearbit.com/virginatlantic.com",
    "NH": "https://logo.clearbit.com/ana.co.jp",
    "JL": "https://logo.clearbit.com/jal.com",
    "SQ": "https://logo.clearbit.com/singaporeair.com",
    "CX": "https://logo.clearbit.com/cathaypacific.com",
    "EK": "https://logo.clearbit.com/emirates.com",
    "EY": "https://logo.clearbit.com/etihad.com",
    "QR": "https://logo.clearbit.com/qatarairways.com",
    "TK": "https://logo.clearbit.com/turkishairlines.com",
    "OS": "https://logo.clearbit.com/austrian.com",
    "SK": "https://logo.clearbit.com/sas.dk",
    "AY": "https://logo.clearbit.com/finnair.com",
    "IB": "https://logo.clearbit.com/iberia.com",
    "TP": "https://logo.clearbit.com/tapportugal.com",
    "KE": "https://logo.clearbit.com/koreanair.com",
    "OZ": "https://logo.clearbit.com/asiana.com",
    "TG": "https://logo.clearbit.com/thaiairways.com",
    "MH": "https://logo.clearbit.com/malaysiaairlines.com",
    "GA": "https://logo.clearbit.com/garuda-indonesia.com",
    "AI": "https://logo.clearbit.com/airindia.com",
    "WY": "https://logo.clearbit.com/omanair.com",
    "RJ": "https://logo.clearbit.com/rj.com",
    "SV": "https://logo.clearbit.com/saudia.com",
    "ET": "https://logo.clearbit.com/ethiopianairlines.com",
    "QF": "https://logo.clearbit.com/qantas.com",
    "NZ": "https://logo.clearbit.com/airnewzealand.com",
    "B6": "https://logo.clearbit.com/jetblue.com",
    "WS": "https://logo.clearbit.com/westjet.com",
    "DE": "https://logo.clearbit.com/condor.com",
    "FI": "https://logo.clearbit.com/icelandair.com",
    "BT": "https://logo.clearbit.com/airbaltic.com",
    "AZ": "https://logo.clearbit.com/ita-airways.com",
    "CM": "https://logo.clearbit.com/copa.com",
    "LA": "https://logo.clearbit.com/latam.com",
    "G3": "https://logo.clearbit.com/voegol.com.br",
}

# ── Real cash price lookups via Google Flights (fast-flights) ─────────────────
_cash_price_cache = {}
CASH_PRICE_TTL    = 6 * 3600  # 6 hours

_GF_SEAT_MAP = {
    "economy":  "economy",
    "premium":  "premium-economy",
    "business": "business",
    "first":    "first",
}

_IATA_TO_GF_NAME = {
    "AA": {"American"},
    "AC": {"Air Canada"},
    "AF": {"Air France"},
    "AI": {"Air India"},
    "AK": {"AirAsia"},
    "AS": {"Alaska", "Alaska Airlines"},
    "AY": {"Finnair"},
    "AZ": {"ITA Airways"},
    "B6": {"JetBlue"},
    "BA": {"British Airways"},
    "BT": {"airBaltic"},
    "CM": {"Copa Airlines"},
    "CX": {"Cathay Pacific"},
    "DE": {"Condor"},
    "DL": {"Delta"},
    "EI": {"Aer Lingus"},
    "EK": {"Emirates"},
    "ET": {"Ethiopian"},
    "EY": {"Etihad"},
    "FI": {"Icelandair"},
    "G3": {"GOL"},
    "GA": {"Garuda Indonesia"},
    "HX": {"Hong Kong Airlines"},
    "IB": {"Iberia"},
    "JL": {"Japan Airlines"},
    "KE": {"Korean Air"},
    "KL": {"KLM"},
    "KU": {"Kuwait Airways"},
    "LA": {"LATAM"},
    "LH": {"Lufthansa"},
    "LX": {"SWISS", "Swiss"},
    "MH": {"Malaysia Airlines"},
    "MS": {"EgyptAir"},
    "MU": {"China Eastern"},
    "NH": {"ANA", "All Nippon Airways"},
    "NZ": {"Air New Zealand"},
    "OS": {"Austrian"},
    "OZ": {"Asiana"},
    "PR": {"Philippine Airlines"},
    "QF": {"Qantas"},
    "QR": {"Qatar Airways"},
    "RJ": {"Royal Jordanian"},
    "SK": {"SAS", "Scandinavian"},
    "SN": {"Brussels Airlines"},
    "SQ": {"Singapore Airlines"},
    "SV": {"Saudia"},
    "TG": {"Thai Airways"},
    "TK": {"Turkish Airlines"},
    "TP": {"TAP Air Portugal"},
    "UA": {"United"},
    "UX": {"Air Europa"},
    "VN": {"Vietnam Airlines"},
    "VS": {"Virgin Atlantic"},
    "WS": {"WestJet"},
    "WY": {"Oman Air"},
}


def _airlines_match(gf_name, iata_codes):
    gf_lower = gf_name.lower()
    for code in iata_codes:
        names = _IATA_TO_GF_NAME.get(code.upper(), set())
        for n in names:
            if n.lower() in gf_lower or gf_lower in n.lower():
                return True
    return False


def fetch_cash_price(origin, dest, date, cabin, airlines=None, direct=None):
    airlines_key = tuple(sorted(a.upper() for a in airlines)) if airlines else ()
    direct_key   = bool(direct)
    key = (origin, dest, date, cabin, airlines_key, direct_key)
    now = time.time()
    cached = _cash_price_cache.get(key)
    if cached is not None and (now - cached[1]) < CASH_PRICE_TTL:
        return cached[0]

    seat     = _GF_SEAT_MAP.get(cabin, "business")
    max_stop = 0 if direct else None
    try:
        result = gf_get_flights(
            flight_data=[FlightData(date=date, from_airport=origin, to_airport=dest)],
            trip="one-way",
            passengers=Passengers(adults=1),
            seat=seat,
            fetch_mode="fallback",
            max_stops=max_stop,
        )
        prices = []
        for f in (result.flights or []):
            if direct and f.stops != 0:
                continue
            if airlines and not _airlines_match(f.name, airlines):
                continue
            if f.price and "$" in f.price:
                try:
                    prices.append(int(f.price.replace("$", "").replace(",", "")))
                except ValueError:
                    pass
        price = min(prices) if prices else None
    except Exception as e:
        print(f"[cash-price] {origin}→{dest} {date} {cabin} airlines={airlines}: {e}")
        price = None

    _cash_price_cache[key] = (price, now)
    return price


def prefetch_cash_prices(combos, max_workers=5):
    to_fetch = []
    now = time.time()
    for combo in set(combos):
        origin, dest, date, cabin = combo[:4]
        airlines = combo[4] if len(combo) > 4 else None
        direct   = combo[5] if len(combo) > 5 else None
        airlines_key = tuple(sorted(a.upper() for a in airlines)) if airlines else ()
        direct_key   = bool(direct)
        cache_key = (origin, dest, date, cabin, airlines_key, direct_key)
        cached = _cash_price_cache.get(cache_key)
        if cached is None or (now - cached[1]) >= CASH_PRICE_TTL:
            to_fetch.append((origin, dest, date, cabin, airlines, direct))

    if not to_fetch:
        return

    def _fetch(args):
        fetch_cash_price(*args)

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = [pool.submit(_fetch, args) for args in to_fetch]
        concurrent.futures.wait(futures, timeout=4)
    finally:
        pool.shutdown(wait=False)  # don't block — abandon any still-running GF threads


def google_flights_url_simple(origin, destination, date, cabin="business", direct=False):
    cabin_label = {
        "economy": "economy",
        "premium": "premium economy",
        "business": "business class",
        "first":   "first class",
    }.get(cabin, "business class")
    nonstop_str = " nonstop" if direct else ""
    query = f"one way{nonstop_str} {cabin_label} flight {origin} to {destination} {date}"
    return f"https://www.google.com/travel/flights?q={urllib.parse.quote(query)}&curr=USD"


def kayak_url(origin, destination, date, cabin="business", direct=False):
    cabin_map = {"economy": "e", "premium": "w", "business": "b", "first": "f"}
    c = cabin_map.get(cabin, "b")
    base = f"https://www.kayak.com/flights/{origin}-{destination}/{date}/1adults/{c}"
    if direct:
        base += "?fs=stops=0"
    return base


# ── seats.aero call counter ───────────────────────────────────────────────────
import fcntl

_COUNTER_FILE  = os.environ.get("SEATS_COUNTER_FILE", "/tmp/seats_aero_counter.json")
_COUNTER_ALERT = int(os.environ.get("SEATS_COUNTER_ALERT", 200))  # notify at this many calls
_APPA_HOOK_URL = os.environ.get("APPA_HOOK_URL", "https://hooks.airbitrage.io/hooks/wake")
_APPA_TOKEN    = os.environ.get("APPA_TOKEN", "flightdash-hook-token-2026")

def _increment_call_counter():
    """Increment the seats.aero call counter (file-based, safe across gunicorn workers).
    Sends a notification when the daily count crosses SEATS_COUNTER_ALERT."""
    import datetime
    today = datetime.date.today().isoformat()
    try:
        with open(_COUNTER_FILE, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.seek(0)
            raw = f.read()
            try:
                data = json.loads(raw) if raw.strip() else {}
            except Exception:
                data = {}
            # Reset counter if it's a new day
            if data.get("date") != today:
                data = {"date": today, "count": 0, "alerted": False}
            data["count"] += 1
            count    = data["count"]
            alerted  = data.get("alerted", False)
            # Write back
            f.seek(0)
            f.truncate()
            f.write(json.dumps(data))
            fcntl.flock(f, fcntl.LOCK_UN)
        # Send alert (outside lock) if threshold just crossed
        if count >= _COUNTER_ALERT and not alerted:
            _mark_alerted_and_notify(today, count)
    except Exception as e:
        print(f"[counter] error: {e}")

def _mark_alerted_and_notify(today, count):
    """Mark alerted=True in the counter file and send a notification to Appa."""
    try:
        with open(_COUNTER_FILE, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            raw = f.read()
            try:
                data = json.loads(raw) if raw.strip() else {}
            except Exception:
                data = {}
            if not data.get("alerted"):
                data["alerted"] = True
                f.seek(0); f.truncate()
                f.write(json.dumps(data))
            fcntl.flock(f, fcntl.LOCK_UN)
        # Notify Appa
        msg = (f"⚠️ seats.aero API alert: {count} calls made today ({today}). "
               f"Approaching daily rate limit — check usage at airbitrage.io.")
        _requests.post(
            _APPA_HOOK_URL,
            headers={"Authorization": f"Bearer {_APPA_TOKEN}", "Content-Type": "application/json"},
            json={"text": msg, "mode": "now"},
            timeout=5,
        )
        print(f"[counter] alert sent: {count} calls today")
    except Exception as e:
        print(f"[counter] alert error: {e}")


# ── seats.aero helpers ─────────────────────────────────────────────────────────
def curl_get(url):
    try:
        resp = _requests.get(url, headers={
            "Partner-Authorization": SEATS_AERO_KEY,
            "accept": "application/json",
        }, timeout=30)
        _increment_call_counter()
        # Log every seats.aero call for audit
        import urllib.parse as _up
        parsed = _up.urlparse(url)
        qs     = _up.parse_qs(parsed.query)
        origin = qs.get("origin_airport", [""])[0]
        dest   = qs.get("destination_airport", [""])[0]
        start  = qs.get("start_date", [""])[0]
        end    = qs.get("end_date", [""])[0]
        cabins = qs.get("cabins", [""])[0]
        sources= qs.get("sources", [""])[0]
        print(f"[seats.aero] {parsed.path} origin={origin} dest={dest} {start}-{end} cabins={cabins} sources={sources} status={resp.status_code}")
        return resp.json()
    except Exception as e:
        print(f"[seats.aero] ERROR: {e} url={url[:100]}")
        return None


def fetch_trips(availability_id, direct_only=False, carriers_filter=None):
    data = curl_get(f"{TRIPS_URL}/{availability_id}")
    if not data:
        return []
    trips = data if isinstance(data, list) else data.get("data", [])

    filter_codes = set()
    if carriers_filter:
        for c in carriers_filter.replace(" ", "").split(","):
            if c:
                filter_codes.add(c.upper())

    results = []
    for trip in trips:
        stops         = trip.get("Stops", 0)
        carriers      = trip.get("Carriers", "")
        carrier_codes = set(c.strip().upper() for c in carriers.split(",") if c.strip())

        if direct_only and stops != 0:
            continue
        if filter_codes and not filter_codes.intersection(carrier_codes):
            continue

        segments = trip.get("AvailabilitySegments") or []
        seg_details = []
        for seg in segments:
            airline_code = seg.get("FlightNumber", "")[:2] if seg.get("FlightNumber") else ""
            seg_details.append({
                "flight_number": seg.get("FlightNumber", ""),
                "airline_code":  airline_code,
                "airline_logo":  CARRIER_LOGOS.get(airline_code, ""),
                "origin":        seg.get("OriginAirport", ""),
                "destination":   seg.get("DestinationAirport", ""),
                "departs_at":    seg.get("DepartsAt", ""),
                "arrives_at":    seg.get("ArrivesAt", ""),
                "duration_min":  seg.get("Duration") or None,
                "aircraft_code": seg.get("AircraftCode", ""),
                "aircraft_name": seg.get("AircraftName", ""),
                "fare_class":    seg.get("FareClass", ""),
            })

        results.append({
            "trip_id":            trip.get("ID", ""),
            "cabin":              trip.get("Cabin", ""),
            "miles":              trip.get("MileageCost", 0),
            "taxes_raw":          trip.get("TotalTaxes", 0),
            "taxes_currency":     trip.get("TaxesCurrency", "USD"),
            "total_duration_min": trip.get("TotalDuration", 0),
            "stops":              stops,
            "carriers":           carriers,
            "flight_numbers":     trip.get("FlightNumbers", ""),
            "departs_at":         trip.get("DepartsAt", ""),
            "arrives_at":         trip.get("ArrivesAt", ""),
            "remaining_seats":    trip.get("RemainingSeats", 0),
            "segments":           seg_details,
        })
    return results


def taxes_to_usd(raw, currency):
    return (raw / 100) * TAX_FX.get(currency, 1.0)


ALLOWED_PROGRAMS = {"aeroplan", "alaska", "american", "virginatlantic", "flyingblue"}

def search_seats_aero(origins, destinations, date_from, date_to, cabins, programs):
    origin_str  = ",".join(origins) if isinstance(origins, list) else origins
    dest_str    = ",".join(destinations) if isinstance(destinations, list) else destinations
    cabin_str   = ",".join(cabins) if isinstance(cabins, list) else cabins
    effective   = [p for p in programs if p in ALLOWED_PROGRAMS] if programs else list(ALLOWED_PROGRAMS)
    sources_str = ",".join(effective)

    url = (
        f"{BASE_URL}"
        f"?origin_airport={urllib.parse.quote(origin_str)}"
        f"&destination_airport={urllib.parse.quote(dest_str)}"
        f"&start_date={date_from}&end_date={date_to}"
        f"&cabins={urllib.parse.quote(cabin_str)}"
        f"&sources={urllib.parse.quote(sources_str)}"
        f"&order_by=lowest_mileage&take=500"
    )
    data = curl_get(url)
    if not data:
        return []
    return data.get("data", []) if isinstance(data, dict) else []


def score_row(row, cabin_pref):
    source     = row.get("Source", "")
    info       = BUY_MILES_INFO.get(source, {})
    rate       = info.get("standard_cpp_usd", 2.0)
    # Single source of truth for displayed price — always use CURRENT_PROMO_CPP
    promo_rate = CURRENT_PROMO_CPP.get(source, info.get("promo_cpp_usd", rate * 0.65))
    route      = row.get("Route", {})
    currency   = row.get("TaxesCurrency", "USD")
    distance   = route.get("Distance", 0)

    cabin_map = [("first", "F"), ("business", "J"), ("premium", "W"), ("economy", "Y")]
    if cabin_pref and cabin_pref != "any":
        check = [{"first": ("first", "F"), "business": ("business", "J"),
                  "premium": ("premium", "W"), "economy": ("economy", "Y")
                  }.get(cabin_pref, ("business", "J"))]
    else:
        check = cabin_map

    for cabin_name, prefix in check:
        if not row.get(f"{prefix}Available"):
            continue
        miles = int(row.get(f"{prefix}MileageCost") or 0)
        if miles <= 0:
            continue

        taxes_usd   = taxes_to_usd(row.get(f"{prefix}TotalTaxesRaw", 0), currency)
        buy_usd     = (miles * rate) / 100
        buy_promo   = (miles * promo_rate) / 100
        svc_mult    = PROGRAM_SERVICE_FEE.get(source, 1.0)
        total_usd   = (buy_usd + taxes_usd) * svc_mult
        total_promo = (buy_promo + taxes_usd) * svc_mult

        orig_code     = route.get("OriginAirport", "")
        dest_code     = route.get("DestinationAirport", "")
        date_str      = row.get("Date", "")
        is_direct     = bool(row.get(f"{prefix}Direct", False))
        airline_codes = [a.strip() for a in (row.get(f"{prefix}AirlinesRaw") or "").split(",") if a.strip()]
        airlines_key  = tuple(sorted(a.upper() for a in airline_codes)) if airline_codes else ()
        cache_key     = (orig_code, dest_code, date_str, cabin_name, airlines_key, is_direct)
        cache_hit     = _cash_price_cache.get(cache_key)
        cash_est      = cache_hit[0] if (cache_hit and cache_hit[0] is not None) else None
        cash_est_source = "google_flights" if cash_est is not None else "unavailable"
        cash_for_math   = cash_est or 0

        savings = round(cash_for_math - total_usd, 0) if cash_est else None
        ratio   = round(cash_for_math / total_usd, 2) if (cash_est and total_usd > 0) else None

        carrier_logos = {code: CARRIER_LOGOS[code] for code in airline_codes if code in CARRIER_LOGOS}

        buy_info = {
            "program_name":        info.get("program_name", source),
            "logo_url":            info.get("logo_url", ""),
            "buy_url":             info.get("buy_url", ""),
            "standard_cpp_usd":    rate,
            "promo_cpp_usd":       promo_rate,
            "typical_promo_bonus": info.get("typical_promo_bonus", 40),
            "currency":            info.get("currency", "USD"),
            "min_purchase":        info.get("min_purchase", 1000),
            "max_purchase":        info.get("max_purchase", 150000),
            "notes":               info.get("notes", ""),
            "cost_at_standard":    round(buy_usd, 2),
            "cost_at_promo":       round(buy_promo, 2),
            "total_at_standard":   round(total_usd, 2),
            "total_at_promo":      round(total_promo, 2),
        }

        return {
            "availability_id":           row.get("ID", ""),
            "date":                      row.get("Date", ""),
            "origin":                    orig_code,
            "destination":               dest_code,
            "distance_miles":            distance,
            "program":                   source,
            "program_name":              info.get("program_name", source),
            "cabin":                     cabin_name,
            "miles":                     miles,
            "taxes_usd":                 round(taxes_usd, 2),
            "arb_miles_cost_usd":        round(buy_usd, 2),
            "arb_miles_cost_promo_usd":  round(buy_promo, 2),
            "arb_price_usd":             round(total_usd, 2),
            "arb_price_promo_usd":       round(total_promo, 2),
            "cash_price_usd":            cash_est,
            "cash_price_source":         cash_est_source,
            "savings_usd":               savings,
            "value_ratio":               ratio,
            "airlines":                  row.get(f"{prefix}AirlinesRaw", ""),
            "carrier_logos":             carrier_logos,
            "program_logo_url":          info.get("logo_url", ""),
            "direct":                    is_direct,
            "remaining_seats":           row.get(f"{prefix}RemainingSeats", 0),
            "taxes_currency":            currency,
            "buy_miles_info":            buy_info,
            "google_flights_url":        google_flights_url_simple(orig_code, dest_code, date_str, cabin_name, is_direct),
            "kayak_url":                 kayak_url(orig_code, dest_code, date_str, cabin_name, is_direct),
        }
    return None


# ── Discover ──────────────────────────────────────────────────────────────────

CITY_NAMES = {
    "JFK": "New York",   "LAX": "Los Angeles",  "SFO": "San Francisco", "BOS": "Boston",
    "ORD": "Chicago",    "MIA": "Miami",         "YYZ": "Toronto",       "YVR": "Vancouver",
    "YUL": "Montreal",   "EWR": "Newark",
    "LHR": "London",     "CDG": "Paris",         "FRA": "Frankfurt",     "AMS": "Amsterdam",
    "ZRH": "Zurich",     "FCO": "Rome",          "MAD": "Madrid",        "LIS": "Lisbon",
    "ARN": "Stockholm",  "VIE": "Vienna",        "MUC": "Munich",        "BCN": "Barcelona",
    "HEL": "Helsinki",   "CPH": "Copenhagen",
    "NRT": "Tokyo",      "HND": "Tokyo",         "ICN": "Seoul",         "HKG": "Hong Kong",
    "PVG": "Shanghai",   "SHA": "Shanghai",      "BKK": "Bangkok",       "SIN": "Singapore",
    "KUL": "Kuala Lumpur",
    "SYD": "Sydney",     "MEL": "Melbourne",     "AKL": "Auckland",
    "DXB": "Dubai",      "DOH": "Doha",          "AUH": "Abu Dhabi",     "RUH": "Riyadh",
    "AMM": "Amman",      "CAI": "Cairo",
    "DEL": "Delhi",      "BOM": "Mumbai",
    "JNB": "Johannesburg", "CPT": "Cape Town",   "ADD": "Addis Ababa",   "NBO": "Nairobi",
    "GRU": "São Paulo",  "EZE": "Buenos Aires",  "BOG": "Bogotá",        "MEX": "Mexico City",
    "LIM": "Lima",       "SCL": "Santiago",
}

REGION_MAP = {
    "LHR": "Europe", "CDG": "Europe", "FRA": "Europe", "AMS": "Europe", "ZRH": "Europe",
    "FCO": "Europe", "MAD": "Europe", "LIS": "Europe", "ARN": "Europe", "VIE": "Europe",
    "MUC": "Europe", "BCN": "Europe", "HEL": "Europe", "CPH": "Europe",
    "NRT": "Asia",   "HND": "Asia",   "ICN": "Asia",   "HKG": "Asia",   "PVG": "Asia",
    "SHA": "Asia",   "BKK": "Asia",   "SIN": "Asia",   "KUL": "Asia",
    "SYD": "Pacific", "MEL": "Pacific", "AKL": "Pacific",
    "DXB": "Middle East", "DOH": "Middle East", "AUH": "Middle East",
    "RUH": "Middle East", "AMM": "Middle East",
    "CAI": "Africa", "JNB": "Africa", "CPT": "Africa", "ADD": "Africa", "NBO": "Africa",
    "DEL": "South Asia", "BOM": "South Asia",
    "GRU": "Latin Am.", "EZE": "Latin Am.", "BOG": "Latin Am.",
    "MEX": "Latin Am.", "LIM": "Latin Am.", "SCL": "Latin Am.",
}

def _load_cpp_from_vault():
    import sqlite3, os
    defaults = {
        "aeroplan":       1.49,
        "alaska":         1.88125,
        "virginatlantic": 1.18,
        "american":       2.26,
        "flyingblue":     1.69,
        "delta":          2.50,
        "united":         2.00,
        "singapore":      1.80,
        "etihad":         1.80,
        "lufthansa":      1.70,
    }
    try:
        db = os.path.join(os.path.dirname(__file__), "vault.db")
        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT program, cost_per_point_cents FROM vault_accounts WHERE status='active'").fetchall()
        conn.close()
        for program, cpp in rows:
            if program and cpp:
                defaults[program] = float(cpp)
    except Exception as e:
        print(f"[cpp] Could not load vault CPP: {e}")
    return defaults

CURRENT_PROMO_CPP = _load_cpp_from_vault()

# Per-program service fee multiplier (applied on top of miles cost + taxes)
# 1.0 = no fee, 1.20 = 20% surcharge
PROGRAM_SERVICE_FEE = {
    "flyingblue":     1.20,
    "american":       1.20,
    "aeroplan":       1.20,
    "alaska":         1.20,
    "virginatlantic": 1.20,
}

DISCOVER_SEARCHES = [
    ("JFK,EWR,BOS,ORD,MIA,LAX,SFO,YYZ", "LHR,CDG,FRA,ZRH,AMS,FCO",
     "aeroplan,alaska,american,virginatlantic", "business,first"),
    ("SIN,HKG,NRT,ICN,BKK,DEL,BOM", "LHR,CDG,FRA,ZRH,AMS,FCO",
     "aeroplan,alaska,american,lufthansa,etihad", "business,first"),
    ("HND,NRT", "BKK,SIN,PVG,ICN,HKG,SYD,LAX,JFK,ORD,LHR",
     "american,aeroplan,alaska", "business,first"),
    ("DXB,DOH,AUH,RUH,CAI,AMM", "LHR,CDG,FRA,ZRH,FCO,ARN",
     "alaska,aeroplan,american,lufthansa", "business"),
    ("ICN,HKG,NRT,SIN,BKK,DEL", "NRT,SIN,HKG,BKK,SYD,MEL",
     "alaska,aeroplan,american", "business,first"),
    ("JFK,LAX,MIA,ORD,SFO,BOS", "NRT,SIN,ICN,HKG,SYD,MEL,BKK",
     "american,aeroplan,alaska", "business,first"),
    ("NRT,HND", "PVG,SHA",
     "aeroplan,alaska,american", "business,first"),
]

# Pinned: (origin, dest, sources, cabins, date_override)
# date_override: "today" = today only; None = 90-day window
PINNED_SEARCHES = [
    ("BOS", "ZRH", "aeroplan", "business,first", None),
    ("EWR", "FRA", "aeroplan", "first",          "today"),
    ("SIN", "LHR", "aeroplan", "business",        None),
]

# ── Manual search results ─────────────────────────────────────────────────────
# Hardcoded results for routes where the Partner API doesn't surface live-only
# availability. Injected into /api/search responses when origin+destination match.
# taxes_usd: convert from currency at time of entry (€515 ≈ $556 at 1.08)
# expires: drop after this date
MANUAL_SEARCH_RESULTS = [
    {
        "origin":              "DXB",
        "destination":         "JFK",
        "program":             "flyingblue",
        "program_name":        "Air France/KLM Flying Blue",
        "cabin":               "business",
        "miles":               90000,
        "taxes_usd":           593,   # confirmed $593.23 on seats.aero Jun 9 2026
        "direct":              False,
        "stops":               1,
        "airlines":            "AF",
        "carriers":            "AF",
        "carrier_logos":       {"AF": CARRIER_LOGOS.get("AF", "")},
        "date":                 "2026-06-09",
        "_date_from":           "2026-06-05",  # valid window start
        "_date_to":             "2026-06-11",  # valid window end
        "departs_at":          "2026-06-09T00:40:00Z",  # AF655 DXB dep 00:40 local
        "arrives_at":          "2026-06-09T10:40:00Z",  # AF2 JFK arr 10:40 AM local
        "flight_numbers":      "AF655, AF2",
        "segments": [
            {"flight_number": "AF655", "airline_code": "AF",
             "airline_logo": CARRIER_LOGOS.get("AF", ""),
             "origin": "DXB", "destination": "CDG",
             "departs_at": "2026-06-09T00:40:00Z", "arrives_at": "2026-06-09T08:10:00Z",
             "duration_min": 450},
            {"flight_number": "AF2", "airline_code": "AF",
             "airline_logo": CARRIER_LOGOS.get("AF", ""),
             "origin": "CDG", "destination": "JFK",
             "departs_at": "2026-06-09T10:40:00Z", "arrives_at": "2026-06-09T18:40:00Z",
             "duration_min": 480},
        ],
        "_cash_price_usd":     2429,
        "note":                "Via CDG — verify availability on seats.aero before booking",
        "_manual":             True,
        "_expires":            "2026-07-01",
        "_cabins":             ["business"],
        "_cpp":                1.69,
        "_service_fee_pct":    0.20,
    },
    {
        "origin":              "DXB",
        "destination":         "EWR",
        "program":             "flyingblue",
        "program_name":        "Air France/KLM Flying Blue",
        "cabin":               "business",
        "miles":               90000,
        "taxes_usd":           593,   # confirmed $593.23 on seats.aero Jun 9 2026
        "direct":              False,
        "stops":               1,
        "airlines":            "AF",
        "carriers":            "AF",
        "carrier_logos":       {"AF": CARRIER_LOGOS.get("AF", "")},
        "date":                 "2026-06-09",
        "_date_from":           "2026-06-05",
        "_date_to":             "2026-06-11",
        "departs_at":          "2026-06-09T00:40:00Z",
        "arrives_at":          "2026-06-09T14:45:00Z",  # AF62 EWR arr 14:45 EDT
        "flight_numbers":      "AF655, AF62",
        "segments": [
            {"flight_number": "AF655", "airline_code": "AF",
             "airline_logo": CARRIER_LOGOS.get("AF", ""),
             "origin": "DXB", "destination": "CDG",
             "departs_at": "2026-06-09T00:40:00Z", "arrives_at": "2026-06-09T08:10:00Z",
             "duration_min": 450},
            {"flight_number": "AF62", "airline_code": "AF",
             "airline_logo": CARRIER_LOGOS.get("AF", ""),
             "origin": "CDG", "destination": "EWR",
             "departs_at": "2026-06-09T10:15:00Z", "arrives_at": "2026-06-09T18:45:00Z",
             "duration_min": 510},
        ],
        "note":                "Via CDG — verify availability on seats.aero before booking",
        "_manual":             True,
        "_expires":            "2026-07-01",
        "_cash_price_usd":     3057,
        "_cabins":             ["business"],
        "_cpp":                1.69,
        "_service_fee_pct":    0.20,
    },
    {
        "origin":              "BCN",
        "destination":         "MNL",
        "program":             "aeroplan",
        "program_name":        "Air Canada Aeroplan",
        "cabin":               "mixed",  # BCN->ICN business (C), ICN->MNL economy (Y)
        "miles":               110000,
        "taxes_usd":           58,    # CA$83 x 0.694
        "direct":              False,
        "stops":               1,
        "airlines":            "OZ",
        "carriers":            "OZ",
        "carrier_logos":       {"OZ": CARRIER_LOGOS.get("OZ", "")},
        "date":                "2026-04-28",
        "_date_from":          "2026-04-28",
        "_date_to":            "2026-04-28",
        "departs_at":          "2026-04-28T20:50:00Z",  # BCN dep 20:50 local
        "arrives_at":          "2026-04-29T22:35:00Z",  # MNL arr 22:35 local next day
        "flight_numbers":      "OZ541, OZ701",
        "segments": [
            {"flight_number": "OZ541", "airline_code": "OZ",
             "airline_logo": CARRIER_LOGOS.get("OZ", ""),
             "origin": "BCN", "destination": "ICN",
             "departs_at": "2026-04-28T20:50:00Z", "arrives_at": "2026-04-29T15:40:00Z",
             "duration_min": 720, "aircraft_name": "Airbus A350-900", "fare_class": "J"},
            {"flight_number": "OZ701", "airline_code": "OZ",
             "airline_logo": CARRIER_LOGOS.get("OZ", ""),
             "origin": "ICN", "destination": "MNL",
             "departs_at": "2026-04-29T18:35:00Z", "arrives_at": "2026-04-29T22:35:00Z",
             "duration_min": 240, "aircraft_name": "Airbus A321", "fare_class": "Y"},
        ],
        "remaining_seats":     3,
        "_cash_price_usd":     3920,  # BCN->ICN OZ biz ($3,648) + ICN->MNL OZ eco ($272)
        "note":                "Via ICN — verify on aircanada.com before booking",
        "_manual":             True,
        "_expires":            "2026-04-29",
        "_cabins":             ["business", "mixed"],  # show for business searches
        "_service_fee_pct":    0.20,
    },
]


def _get_manual_results(origins, destinations, cabin, date_from, date_to):
    """Return manual search results matching the given query."""
    import datetime
    today_str = datetime.date.today().isoformat()
    results = []
    orig_set = set(o.upper() for o in (origins if isinstance(origins, list) else [origins]))
    dest_set = set(d.upper() for d in (destinations if isinstance(destinations, list) else [destinations]))
    for r in MANUAL_SEARCH_RESULTS:
        if r.get("_expires", "9999") < today_str:
            continue
        if r["origin"] not in orig_set or r["destination"] not in dest_set:
            continue
        allowed_cabins = r.get("_cabins", ["business", "first"])
        if cabin and cabin != "any" and cabin not in allowed_cabins:
            continue
        # Only show if the queried date range overlaps the result's valid window
        r_date_from = r.get("_date_from", r.get("date", date_from))
        r_date_to   = r.get("_date_to",   r.get("date", date_to))
        if date_to < r_date_from or date_from > r_date_to:
            continue
        cpp          = r.get("_cpp") or CURRENT_PROMO_CPP.get(r["program"], 2.0)
        svc_fee_pct  = r.get("_service_fee_pct", 0.0)
        miles = r["miles"]
        taxes = r["taxes_usd"]
        info  = BUY_MILES_INFO.get(r["program"], {})
        buy_promo    = round(miles * cpp / 100, 2)
        subtotal     = round(buy_promo + taxes, 2)
        total        = round(subtotal * (1 + svc_fee_pct), 2)
        result = {
            "availability_id":          "",
            "date":                     r.get("date", date_from),
            "origin":                   r["origin"],
            "destination":              r["destination"],
            "program":                  r["program"],
            "program_name":             r["program_name"],
            "cabin":                    r["cabin"],
            "miles":                    miles,
            "taxes_usd":                taxes,
            "arb_miles_cost_promo_usd": buy_promo,
            "arb_price_promo_usd":      total,
            "arb_miles_cost_usd":       round(miles * info.get("standard_cpp_usd", cpp) / 100, 2),
            "arb_price_usd":            round(miles * info.get("standard_cpp_usd", cpp) / 100 + taxes, 2),
            "cash_price_usd":           None,
            "cash_price_source":        "unavailable",
            "savings_usd":              None,
            "value_ratio":              None,
            "direct":                   r["direct"],
            "airlines":                 r["airlines"],
            "carrier_logos":            r["carrier_logos"],
            "program_logo_url":         info.get("logo_url", ""),
            "buy_miles_info":           {
                "program_name":        info.get("program_name", r["program_name"]),
                "logo_url":            info.get("logo_url", ""),
                "buy_url":             info.get("buy_url", ""),
                "standard_cpp_usd":    info.get("standard_cpp_usd", cpp),
                "promo_cpp_usd":       cpp,
                "cost_at_promo":       buy_promo,
                "total_at_promo":      total,
            },
            "google_flights_url":       google_flights_url_simple(r["origin"], r["destination"], date_from, r["cabin"], False),
            "kayak_url":               kayak_url(r["origin"], r["destination"], date_from, r["cabin"], False),
            "note":                     r.get("note", ""),
            "stops":                    r.get("stops", 1),
            "carriers":                 r.get("carriers", ""),
            "flight_numbers":           r.get("flight_numbers", ""),
            "segments":                 r.get("segments", []),
            "departs_at":               r.get("departs_at"),
            "arrives_at":               r.get("arrives_at"),
            "remaining_seats":          r.get("remaining_seats", 0),
            "aircraft_name":            r.get("aircraft_name", ""),
            "cash_price_usd":           r.get("_cash_price_usd"),
            "cash_price_source":        "manual" if r.get("_cash_price_usd") else "unavailable",
            "savings_usd":              round(r["_cash_price_usd"] - total, 0) if r.get("_cash_price_usd") else None,
            "value_ratio":              round(r["_cash_price_usd"] / total, 2) if r.get("_cash_price_usd") else None,
            "_manual":                  True,
        }
        results.append(result)
    return results


_discover_cache = {"tiles": [], "ts": 0.0}
# Discover tiles are rebuilt once daily via POST /api/discover/refresh (cron).
# TTL is a fallback safety net only — not the primary refresh trigger.
DISCOVER_TTL          = int(os.environ.get("DISCOVER_TTL_SECONDS", 86400))  # 24h default
DISCOVER_CACHE_FILE   = os.environ.get("DISCOVER_CACHE_FILE", "/tmp/discover_cache.json")
DISCOVER_REFRESH_TOKEN = os.environ.get("DISCOVER_REFRESH_TOKEN", "discover-refresh-token-2026")

def _load_discover_cache_from_disk():
    """Load persisted discover cache on startup. Ignore if > 24h old."""
    try:
        with open(DISCOVER_CACHE_FILE, "r") as f:
            saved = json.load(f)
        age = time.time() - saved.get("ts", 0)
        if age < 86400 and saved.get("tiles"):
            _discover_cache["tiles"] = saved["tiles"]
            _discover_cache["ts"]    = saved["ts"]
            print(f"[discover] loaded {len(saved['tiles'])} tiles from disk cache (age {int(age/60)}m)")
    except Exception:
        pass  # no cache yet, fine

def _save_discover_cache_to_disk(tiles):
    """Persist discover cache to disk so restarts don't burn API quota."""
    try:
        with open(DISCOVER_CACHE_FILE, "w") as f:
            json.dump({"tiles": tiles, "ts": time.time()}, f)
        print(f"[discover] saved {len(tiles)} tiles to disk cache")
    except Exception as e:
        print(f"[discover] warning: could not save cache to disk: {e}")

_load_discover_cache_from_disk()

DISCOVER_ENABLED = os.environ.get("DISCOVER_ENABLED", "false").lower() == "true"

def _maybe_startup_refresh():
    """On startup, if cache is empty:
    - If disk cache is less than 6 hours old, just load from disk (skip API calls).
    - Otherwise one worker does a synchronous refresh; others wait.
    This prevents deploys from burning seats.aero quota."""
    if not DISCOVER_ENABLED:
        return
    if _discover_cache["tiles"]:
        return
    # Check disk cache age before hitting the API
    try:
        with open(DISCOVER_CACHE_FILE, "r") as f:
            saved = json.load(f)
        age = time.time() - saved.get("ts", 0)
        if age < 6 * 3600 and saved.get("tiles"):  # less than 6h old — use disk, skip API
            _discover_cache["tiles"] = saved["tiles"]
            _discover_cache["ts"]    = saved["ts"]
            print(f"[discover] startup: loaded {len(saved['tiles'])} tiles from disk (age {int(age/60)}m), skipping API refresh")
            return
    except Exception:
        pass  # no cache file, proceed to refresh
    lock_file = "/tmp/discover_refresh.lock"
    done_file = "/tmp/discover_refresh.done"
    # If done_file exists, another worker already finished — just load from disk.
    if os.path.exists(done_file):
        _load_discover_cache_from_disk()
        return
    try:
        fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        # We won the lock — do a synchronous refresh
        print("[discover] no cache on startup — running synchronous refresh")
        tiles = build_discover_tiles()
        _discover_cache["tiles"] = tiles
        _discover_cache["ts"]    = time.time()
        _save_discover_cache_to_disk(tiles)
        # Signal other workers that we're done
        open(done_file, "w").close()
        try:
            os.remove(lock_file)
        except Exception:
            pass
    except FileExistsError:
        # Another worker is refreshing — wait for it then load from disk
        print("[discover] waiting for another worker to finish refresh...")
        for _ in range(60):  # wait up to 60s
            time.sleep(2)
            if os.path.exists(done_file):
                _load_discover_cache_from_disk()
                return
        print("[discover] gave up waiting, starting up without cache")


def build_discover_tiles():
    import datetime
    today     = datetime.date.today()
    start     = today.strftime("%Y-%m-%d")
    end       = (today + datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")

    all_rows = []

    for origins, dests, sources, cabins in DISCOVER_SEARCHES:
        url = (
            f"{BASE_URL}"
            f"?origin_airport={urllib.parse.quote(origins)}"
            f"&destination_airport={urllib.parse.quote(dests)}"
            f"&start_date={start}&end_date={end}"
            f"&cabins={urllib.parse.quote(cabins)}"
            f"&sources={urllib.parse.quote(sources)}"
            f"&order_by=lowest_mileage&take=300"
        )
        data = curl_get(url)
        if data:
            all_rows.extend(data.get("data", []) if isinstance(data, dict) else data)
        time.sleep(0.3)

    for orig, dest, sources, cabins, date_override in PINNED_SEARCHES:
        ps = pe = today_str if date_override == "today" else (start, end)
        if isinstance(ps, tuple):
            ps, pe = ps
        url = (
            f"{BASE_URL}"
            f"?origin_airport={urllib.parse.quote(orig)}"
            f"&destination_airport={urllib.parse.quote(dest)}"
            f"&start_date={ps}&end_date={pe}"
            f"&cabins={urllib.parse.quote(cabins)}"
            f"&sources={urllib.parse.quote(sources)}"
            f"&order_by=lowest_mileage&take=50"
        )
        data = curl_get(url)
        if data:
            rows = data.get("data", []) if isinstance(data, dict) else data
            for r in rows:
                r["_pinned"] = True
            all_rows.extend(rows)
        time.sleep(0.3)

    # First pass: best candidate per (orig, dest, source, cabin) key
    best = {}
    for row in all_rows:
        source    = row.get("Source", "")
        route     = row.get("Route", {})
        promo_cpp = CURRENT_PROMO_CPP.get(source, 2.0)
        currency  = row.get("TaxesCurrency", "USD")

        for cabin_name, prefix in [("first", "F"), ("business", "J")]:
            if not row.get(f"{prefix}Available"):
                continue
            miles = int(row.get(f"{prefix}MileageCost") or 0)
            if miles <= 0:
                continue

            taxes_usd     = taxes_to_usd(row.get(f"{prefix}TotalTaxesRaw", 0), currency)
            buy_promo     = (miles * promo_cpp) / 100
            svc_mult      = PROGRAM_SERVICE_FEE.get(source, 1.0)
            total         = (buy_promo + taxes_usd) * svc_mult
            orig          = route.get("OriginAirport", "?")
            dest          = route.get("DestinationAirport", "?")
            date          = row.get("Date", "")
            key           = (orig, dest, source, cabin_name)
            airline_codes = [a.strip() for a in (row.get(f"{prefix}AirlinesRaw") or "").split(",") if a.strip()]
            is_direct     = bool(row.get(f"{prefix}Direct", False))
            is_pinned     = bool(row.get("_pinned"))

            if key not in best or total < best[key]["_total"] or (is_pinned and not best[key].get("_pinned")):
                best[key] = {
                    "origin_code":              orig,
                    "origin_city":              CITY_NAMES.get(orig, orig),
                    "destination_code":         dest,
                    "destination_city":         CITY_NAMES.get(dest, dest),
                    "region":                   REGION_MAP.get(dest, "Intl"),
                    "date":                     date,
                    "cabin":                    cabin_name,
                    "miles":                    miles,
                    "taxes_usd":                round(taxes_usd),
                    "arb_miles_cost_promo_usd": round(buy_promo),
                    "arb_price_promo_usd":      round(total),
                    "program":                  source,
                    "program_name":             BUY_MILES_INFO.get(source, {}).get("program_name", source),
                    "direct":                   is_direct,
                    "airlines":                 airline_codes,
                    "remaining_seats":          row.get(f"{prefix}RemainingSeats", 0),
                    "availability_exists":      True,
                    "availability_id":          row.get("ID", ""),
                    "_total":                   total,
                    "_airlines":                airline_codes,
                    "_pinned":                  is_pinned,
                }

    candidates = list(best.values())

    # Prefetch cash prices concurrently
    combos = set()
    for t in candidates:
        airlines     = t.get("_airlines") or None
        airlines_key = tuple(sorted(a.upper() for a in airlines)) if airlines else None
        combos.add((t["origin_code"], t["destination_code"], t["date"], t["cabin"],
                    airlines_key, t["direct"]))
    print(f"[discover] fetching {len(combos)} cash prices from Google Flights...")
    prefetch_cash_prices(combos)

    # Second pass: score with real cash prices
    for t in candidates:
        orig, dest, date, cabin = t["origin_code"], t["destination_code"], t["date"], t["cabin"]
        airlines_key = tuple(sorted(a.upper() for a in t.get("_airlines", [])))
        cache_key    = (orig, dest, date, cabin, airlines_key, bool(t["direct"]))
        cache_hit    = _cash_price_cache.get(cache_key)
        cash_price   = cache_hit[0] if (cache_hit and cache_hit[0] is not None) else None
        total        = t["arb_price_promo_usd"]
        t["cash_price_usd"]    = cash_price
        t["savings_usd"]       = round(cash_price - total, 0) if cash_price else None
        t["value_ratio"]       = round(cash_price / total, 2) if (cash_price and total > 0) else None
        t["cash_price_source"] = "google_flights" if cash_price else "unavailable"
        t["_ratio"]            = t["value_ratio"] or 0

    # Filter: drop tiles where ratio < 1.2 (not worth it vs buying a cash ticket)
    # Exception: keep pinned routes and tiles with no cash price (can't judge them)
    MIN_RATIO = 1.2
    def _keep(c):
        if c.get("_pinned"):
            return True
        ratio = c.get("_ratio", 0)
        if ratio == 0:  # no cash price — keep, but sort to back
            return True
        return ratio >= MIN_RATIO

    candidates = [c for c in candidates if _keep(c)]

    # Sort: tiles with cash prices (sortable by ratio) first, then no-cash tiles by miles cost
    pinned_with_cash    = sorted([c for c in candidates if c.get("_pinned") and c["_ratio"] > 0],     key=lambda x: x["_ratio"], reverse=True)
    pinned_no_cash      = sorted([c for c in candidates if c.get("_pinned") and c["_ratio"] == 0],    key=lambda x: x["arb_price_promo_usd"])
    unpinned_with_cash  = sorted([c for c in candidates if not c.get("_pinned") and c["_ratio"] > 0], key=lambda x: x["_ratio"], reverse=True)
    unpinned_no_cash    = sorted([c for c in candidates if not c.get("_pinned") and c["_ratio"] == 0], key=lambda x: x["arb_price_promo_usd"])

    ordered = pinned_with_cash + unpinned_with_cash + pinned_no_cash + unpinned_no_cash
    sorted_tiles = ordered[:20]

    for t in sorted_tiles:
        t.pop("_ratio", None)
        t.pop("_total", None)
        t.pop("_airlines", None)
        t.pop("_pinned", None)

    # Enrich with trip segment details
    def _enrich_tile(t):
        avail_id = t.get("availability_id", "")
        if not avail_id:
            return
        tile_airlines = set(a.upper() for a in (t.get("airlines") or []))
        is_direct     = t.get("direct", False)
        all_trips     = fetch_trips(avail_id)
        if not all_trips:
            return

        matched = [tr for tr in all_trips
                   if tile_airlines and
                   any(c.strip().upper() in tile_airlines for c in tr.get("carriers", "").split(","))]
        if not matched:
            matched = all_trips

        pool_trips = [tr for tr in matched if tr["stops"] == 0] if is_direct else matched
        if not pool_trips:
            pool_trips = matched

        best_trip = sorted(pool_trips, key=lambda x: (x["stops"], x["total_duration_min"]))[0]
        segs = best_trip.get("segments", [])
        main_seg = segs[-1] if len(segs) > 1 else (segs[0] if segs else None)

        t["departs_at"]    = best_trip.get("departs_at") or None
        t["arrives_at"]    = best_trip.get("arrives_at") or None
        t["stops"]         = best_trip.get("stops", 0)
        t["carriers"]      = best_trip.get("carriers", "")
        t["flight_numbers"] = best_trip.get("flight_numbers", "")
        t["segments"]      = segs
        t["aircraft_name"] = main_seg.get("aircraft_name") or None if main_seg else None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(_enrich_tile, sorted_tiles))

    return sorted_tiles


# ── Flask routes ───────────────────────────────────────────────────────────────

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


# On startup, populate cache synchronously so all workers have tiles from the first request.
_maybe_startup_refresh()


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "flight-search-api", "version": "2.0"})


@app.route("/api/seats-usage")
def api_seats_usage():
    """Returns today's seats.aero API call count."""
    import datetime
    today = datetime.date.today().isoformat()
    try:
        with open(_COUNTER_FILE, "r") as f:
            data = json.load(f)
        if data.get("date") != today:
            data = {"date": today, "count": 0, "alerted": False}
    except Exception:
        data = {"date": today, "count": 0, "alerted": False}
    data["alert_threshold"] = _COUNTER_ALERT
    return jsonify(data)


@app.route("/api/discover")
def api_discover():
    if not DISCOVER_ENABLED:
        # Read-only mode: serve cached tiles if available, no rebuilds
        _load_discover_cache_from_disk()
        return jsonify({"tiles": _discover_cache.get("tiles", []), "disabled": True})
    now = time.time()
    if _discover_cache["tiles"] and (now - _discover_cache["ts"]) < DISCOVER_TTL:
        return jsonify({"tiles": _discover_cache["tiles"]})
    # In-memory empty — try disk (another worker may have refreshed).
    _load_discover_cache_from_disk()
    if _discover_cache["tiles"]:
        return jsonify({"tiles": _discover_cache["tiles"]})
    print("[discover] no cache available (use /api/discover/refresh to populate)")
    return jsonify({"tiles": []})


@app.route("/api/discover/patch-cache", methods=["POST", "OPTIONS"])
def api_discover_patch_cache():
    """Directly write a tiles array to the discover cache. No seats.aero calls."""
    if request.method == "OPTIONS":
        return "", 204
    body  = request.get_json(force=True, silent=True) or {}
    token = body.get("token") or request.args.get("token", "")
    if token != DISCOVER_REFRESH_TOKEN:
        return jsonify({"error": "unauthorized"}), 401
    tiles = body.get("tiles")
    if not isinstance(tiles, list):
        return jsonify({"error": "tiles array required"}), 400
    _discover_cache["tiles"] = tiles
    _discover_cache["ts"]    = time.time()
    _save_discover_cache_to_disk(tiles)
    print(f"[discover] cache patched with {len(tiles)} tiles")
    return jsonify({"ok": True, "tiles": len(tiles)})


@app.route("/api/discover/refresh", methods=["POST", "OPTIONS"])
def api_discover_refresh():
    """Force a full discover rebuild. Called by daily 6am cron. Requires token auth."""
    if request.method == "OPTIONS":
        return "", 204
    if not DISCOVER_ENABLED:
        return jsonify({"error": "discover is disabled"}), 503
    body  = request.get_json(force=True, silent=True) or {}
    token = body.get("token") or request.args.get("token", "")
    if token != DISCOVER_REFRESH_TOKEN:
        return jsonify({"error": "unauthorized"}), 401
    print("[discover] refresh triggered by cron")
    tiles = build_discover_tiles()
    _discover_cache["tiles"] = tiles
    _discover_cache["ts"]    = time.time()
    _save_discover_cache_to_disk(tiles)
    return jsonify({"ok": True, "tiles": len(tiles)})


@app.route("/api/trips/<availability_id>")
def api_trips(availability_id):
    direct_only     = request.args.get("direct_only", "false").lower() == "true"
    carriers_filter = request.args.get("carriers")
    trips = fetch_trips(availability_id, direct_only=direct_only, carriers_filter=carriers_filter)
    return jsonify({"trips": trips, "count": len(trips)})


@app.route("/api/search", methods=["POST", "OPTIONS"])
def api_search():
    if request.method == "OPTIONS":
        return "", 204

    body = request.get_json(force=True, silent=True) or {}

    origins      = body.get("origin", [])
    destinations = body.get("destination", [])
    date_from    = body.get("date_from", "")
    date_to      = body.get("date_to", date_from)
    cabin        = body.get("cabin", "business").lower()
    programs     = body.get("programs", [])

    if not origins or not destinations or not date_from:
        return jsonify({"error": "Missing required fields: origin, destination, date_from"}), 400

    if isinstance(origins, str):
        origins = [o.strip() for o in origins.split(",")]
    if isinstance(destinations, str):
        destinations = [d.strip() for d in destinations.split(",")]

    cabin_api_map = {
        "economy": "economy", "premium": "premium",
        "premium economy": "premium", "business": "business",
        "first": "first", "any": "economy,premium,business,first",
    }
    cabins_str = cabin_api_map.get(cabin, "business")

    # flex_only=true: search ±3 days and return only deals cheaper than min_miles threshold
    flex_only      = body.get("flex_only", False)
    min_miles_threshold = int(body.get("min_miles", 0) or 0)

    FLEX_DAYS = 3
    try:
        from datetime import datetime as _dt, timedelta as _td
        _d0 = _dt.strptime(date_from, "%Y-%m-%d").date()
        requested_dates = set()
        for i in range((_dt.strptime(date_to, "%Y-%m-%d").date() - _d0).days + 1):
            requested_dates.add((_d0 + _td(days=i)).strftime("%Y-%m-%d"))
        if flex_only:
            search_from = (_d0 - _td(days=FLEX_DAYS)).strftime("%Y-%m-%d")
            search_to   = (_d0 + _td(days=FLEX_DAYS)).strftime("%Y-%m-%d")
        else:
            search_from, search_to = date_from, date_to
    except Exception:
        flex_only = False
        requested_dates = {date_from}
        search_from, search_to = date_from, date_to

    rows = search_seats_aero(origins, destinations, search_from, search_to, cabins_str, programs)

    cabin_pref  = cabin if cabin != "any" else None
    cabin_lookup = {
        "first": [("first", "F")], "business": [("business", "J")],
        "premium": [("premium", "W")], "economy": [("economy", "Y")],
    }
    cabin_iters = cabin_lookup.get(cabin_pref, [("first","F"),("business","J"),("premium","W"),("economy","Y")])

    combos = set()
    for row in rows:
        route = row.get("Route", {})
        orig  = route.get("OriginAirport", "")
        dest  = route.get("DestinationAirport", "")
        date  = row.get("Date", "")
        for cabin_name, prefix in cabin_iters:
            if row.get(f"{prefix}Available") and int(row.get(f"{prefix}MileageCost") or 0) > 0:
                airline_codes = [a.strip() for a in (row.get(f"{prefix}AirlinesRaw") or "").split(",") if a.strip()]
                is_direct     = bool(row.get(f"{prefix}Direct", False))
                combos.add((orig, dest, date, cabin_name,
                            tuple(sorted(a.upper() for a in airline_codes)) if airline_codes else None,
                            is_direct))

    combos_list = list(combos)[:20]
    if combos_list:
        print(f"[search] fetching {len(combos_list)} cash prices...")
        prefetch_cash_prices(combos_list)

    deals, seen = [], set()
    for row in rows:
        d = score_row(row, cabin_pref)
        if d is None:
            continue
        key = (d["origin"], d["destination"], d["program"], d["cabin"], d["date"])
        if key in seen:
            continue
        seen.add(key)
        deals.append(d)

    if flex_only:
        # Return only alt-date deals cheaper than the given threshold
        if min_miles_threshold > 0:
            deals = [d for d in deals if d["date"] not in requested_dates and d["miles"] < min_miles_threshold]
        else:
            deals = [d for d in deals if d["date"] not in requested_dates]
        for d in deals:
            d["alt_date"] = True

    deals.sort(key=lambda x: (not x["direct"], x["arb_price_usd"]))

    # Enrich top results with trip details (departs_at, arrives_at, segments, flight_numbers)
    def _enrich_result(t):
        avail_id = t.get("availability_id", "")
        if not avail_id:
            return
        try:
            all_trips = fetch_trips(avail_id)
            if not all_trips:
                return
            tile_airlines = set(a.upper() for a in (t.get("airlines") or []))
            is_direct = t.get("direct", False)
            matched = [tr for tr in all_trips
                       if tile_airlines and
                       any(c.strip().upper() in tile_airlines for c in tr.get("carriers", "").split(","))]
            if not matched:
                matched = all_trips
            pool = [tr for tr in matched if tr["stops"] == 0] if is_direct else matched
            if not pool:
                pool = matched
            best = sorted(pool, key=lambda x: (x["stops"], x["total_duration_min"]))[0]
            segs = best.get("segments", [])
            main_seg = segs[-1] if len(segs) > 1 else (segs[0] if segs else None)
            t["departs_at"]    = best.get("departs_at") or None
            t["arrives_at"]    = best.get("arrives_at") or None
            t["stops"]         = best.get("stops", t.get("stops", 0))
            t["carriers"]      = best.get("carriers", "")
            t["flight_numbers"] = best.get("flight_numbers", "")
            t["segments"]      = segs
            t["aircraft_name"] = main_seg.get("aircraft_name") if main_seg else None
        except Exception as e:
            print(f"[search] enrich error for {avail_id}: {e}")

    # Inject manual results (live-only availability not in Partner API cache)
    manual = _get_manual_results(origins, destinations, cabin, date_from, date_to)
    # Only add if not already covered by an API result for same origin+dest+program+cabin
    existing_keys = {(d["origin"], d["destination"], d["program"], d["cabin"]) for d in deals}
    for m in manual:
        key = (m["origin"], m["destination"], m["program"], m["cabin"])
        if key not in existing_keys:
            deals.append(m)

    top = deals[:50]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(_enrich_result, top))

    summary = None
    if deals:
        b = deals[0]
        cash_str = f"~${b['cash_price_usd']:,}" if b.get("cash_price_usd") else "unknown cash price"
        summary = (
            f"Best deal: {b['date']} · {b['program_name']} · "
            f"{b['miles']:,} miles + ${b['taxes_usd']:.0f} taxes "
            f"(arb price ~${b['arb_price_usd']:.0f} standard, "
            f"~${b['arb_price_promo_usd']:.0f} at promo). "
            f"{'Nonstop.' if b['direct'] else 'Connecting.'} "
            f"Cash price: {cash_str}."
        )

    return jsonify({
        "results":     deals[:50],
        "total_found": len(deals),
        "summary":     summary,
        "query": {
            "origins":      origins,
            "destinations": destinations,
            "date_from":    date_from,
            "date_to":      date_to,
            "cabin":        cabin,
            "programs":     programs,
        },
    })


# ── Booking / payment intent ───────────────────────────────────────────────────
@app.route("/api/create-payment-intent", methods=["POST", "OPTIONS"])
def api_create_payment_intent():
    if request.method == "OPTIONS":
        return "", 204

    import stripe
    stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

    body = request.get_json(force=True, silent=True) or {}

    miles         = int(body.get("miles", 0))
    taxes_usd     = float(body.get("taxes_usd", 0))
    cpp_usd       = float(os.getenv("MILES_CPP_USD", "0.0144"))
    svc_fee_cents = int(os.getenv("SERVICE_FEE_CENTS", "3500"))

    miles_cost_cents = int(miles * cpp_usd * 100)
    taxes_cents      = int(taxes_usd * 100)
    total_cents      = miles_cost_cents + taxes_cents + svc_fee_cents

    intent = stripe.PaymentIntent.create(
        amount=total_cents,
        currency="usd",
        metadata={
            "origin":          body.get("origin", ""),
            "destination":     body.get("destination", ""),
            "date":            body.get("date", ""),
            "cabin":           body.get("cabin", ""),
            "availability_id": body.get("availability_id", ""),
            "miles":           str(miles),
            "taxes_usd":       str(taxes_usd),
        },
    )

    return jsonify({
        "client_secret": intent.client_secret,
        "breakdown": {
            "miles_cost_cents": miles_cost_cents,
            "taxes_cents":      taxes_cents,
            "service_fee_cents": svc_fee_cents,
            "total_cents":      total_cents,
        },
    })


@app.route("/api/notify-booking", methods=["POST", "OPTIONS"])
def api_notify_booking():
    if request.method == "OPTIONS":
        return "", 204
    data = request.get_json(force=True, silent=True) or {}
    appa_url = os.getenv("APPA_HOOK_URL", "https://hooks.airbitrage.io/hooks/wake")
    appa_token = os.getenv("APPA_TOKEN", "flightdash-hook-token-2026")

    # Build wake message — include full booking payload so Appa can auto-book
    msg = data.get("text", "")
    if not msg and all(k in data for k in ("first_name", "last_name", "origin", "destination", "date")):
        msg = (
            f"[BOOKING] {data['first_name']} {data['last_name']} · "
            f"DOB: {data.get('dob','?')} · "
            f"{data['origin']}\u2192{data['destination']} {data['date']} ({data.get('cabin','business')}) · "
            f"card: {data.get('card_last4','2002')} · "
            f"filling out {data['origin']}\u2192{data['destination']} {data['date']} ({data.get('cabin','business')})"
        )

    try:
        resp = _requests.post(
            appa_url,
            headers={"Authorization": f"Bearer {appa_token}", "Content-Type": "application/json"},
            json={"text": msg, "mode": "now", "booking": data},
            timeout=10,
        )
        return jsonify({"status": "ok", "appa_status": resp.status_code})
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 502


@app.route("/api/booking-status/<int:booking_id>", methods=["GET", "OPTIONS"])
def api_booking_status(booking_id):
    if request.method == "OPTIONS":
        return "", 204
    try:
        import sqlite3
        from pathlib import Path
        conn = sqlite3.connect(str(Path(__file__).parent / "vault.db"))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
        conn.close()
        if not row:
            return jsonify({"error": "Booking not found"}), 404
        d = dict(row)
        # Expose confirmation_number for frontend polling
        if d.get("status") == "confirmed" and d.get("aeroplan_ref"):
            d["confirmation_number"] = d["aeroplan_ref"]
        return jsonify(d)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Auth ──────────────────────────────────────────────────────────────────
import sqlite3 as _sqlite3
from pathlib import Path as _Path

_DB_PATH = str(_Path(__file__).parent / "vault.db")

def _get_db():
    conn = _sqlite3.connect(_DB_PATH, timeout=10)
    conn.row_factory = _sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def _init_users_table():
    conn = _get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            email         TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            first_name    TEXT NOT NULL,
            last_name     TEXT NOT NULL,
            dob           TEXT,
            gender        TEXT,
            phone         TEXT,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

_init_users_table()

def _user_dict(row):
    return {
        "email":     row["email"],
        "firstName": row["first_name"],
        "lastName":  row["last_name"],
        "dob":       row["dob"],
        "gender":    row["gender"],
        "phone":     row["phone"],
    }


@app.route("/api/auth/signup", methods=["POST", "OPTIONS"])
def api_auth_signup():
    if request.method == "OPTIONS":
        return "", 204
    import bcrypt
    body = request.get_json(force=True, silent=True) or {}
    email      = (body.get("email") or "").strip().lower()
    password   = body.get("password") or ""
    first_name = (body.get("first_name") or "").strip()
    last_name  = (body.get("last_name") or "").strip()
    dob        = body.get("dob") or None
    gender     = body.get("gender") or None
    phone      = body.get("phone") or None

    if not email or not password or not first_name or not last_name:
        return jsonify({"error": "email, password, first_name and last_name are required."}), 400

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    try:
        conn = _get_db()
        conn.execute(
            "INSERT INTO users (email, password_hash, first_name, last_name, dob, gender, phone) VALUES (?,?,?,?,?,?,?)",
            (email, pw_hash, first_name, last_name, dob, gender, phone)
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        conn.close()
        return jsonify({"user": _user_dict(row)}), 200
    except _sqlite3.IntegrityError:
        return jsonify({"error": "An account with that email already exists."}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/auth/login", methods=["POST", "OPTIONS"])
def api_auth_login():
    if request.method == "OPTIONS":
        return "", 204
    import bcrypt
    body     = request.get_json(force=True, silent=True) or {}
    email    = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""

    if not email or not password:
        return jsonify({"error": "email and password are required."}), 400

    conn = _get_db()
    row  = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    conn.close()

    if not row:
        return jsonify({"error": "No account found with that email."}), 404
    if not bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        return jsonify({"error": "Incorrect password."}), 401

    return jsonify({"user": _user_dict(row)}), 200


# ── Booking approval constants ───────────────────────────────────────────────
BOOKING_APPROVE_TOKEN = os.environ.get("BOOKING_APPROVE_TOKEN", "booking-approve-token-2026")
KILL_TOKEN             = os.environ.get("KILL_TOKEN",             "kill-switch-token-2026")


def _appa_notify(text: str):
    """Send a wake-text to Appa's hook."""
    url   = os.getenv("APPA_HOOK_URL",   "https://hooks.airbitrage.io/hooks/wake")
    token = os.getenv("APPA_TOKEN",      "flightdash-hook-token-2026")
    try:
        _requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"text": text, "mode": "now"},
            timeout=8,
        )
    except Exception as e:
        print(f"[appa_notify] failed: {e}")


@app.route("/api/book-complete", methods=["POST", "OPTIONS"])
def api_book_complete():
    """Accept a booking request, store it as pending_approval, notify Appa."""
    if request.method == "OPTIONS":
        return "", 204

    data   = request.get_json(force=True, silent=True) or {}
    flight = data.get("flight", {})
    client = data.get("client", {})

    if not flight or not client:
        return jsonify({"error": "Missing required fields: flight, client"}), 400

    origin      = flight.get("origin", "")
    destination = flight.get("destination", "")
    date        = flight.get("date", "")
    cabin       = flight.get("cabin", "business")
    first_name  = client.get("first_name", "")
    last_name   = client.get("last_name", "")

    if not all([origin, destination, date, first_name, last_name]):
        return jsonify({"error": "Missing required fields in flight or client"}), 400

    # Store as pending_approval — Appa must approve before booking fires
    import json as _json
    conn = _get_db()
    conn.execute(
        "INSERT INTO bookings (vault_id, passenger_name, flight_ref, miles_used, taxes_paid, status) "
        "VALUES (2, ?, ?, 0, 0.0, 'pending_approval')",
        (f"{first_name} {last_name}", f"{origin}-{destination}-{date}"),
    )
    conn.commit()
    booking_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    # Stash full payload in aeroplan_ref for retrieval on approval
    conn.execute("UPDATE bookings SET aeroplan_ref=? WHERE id=?",
                 (_json.dumps({**data, "flight": flight, "client": client}), booking_id))
    conn.commit()
    conn.close()

    # Notify Appa
    msg = (
        f"\U0001f3ab BOOKING REQUEST #{booking_id}\n"
        f"Route: {origin} \u2192 {destination}\n"
        f"Date: {date} | Cabin: {cabin.title()}\n"
        f"Passenger: {first_name} {last_name}\n"
        f"\nApprove or deny:\n"
        f"  approve: POST https://api.airbitrage.io/api/booking-approve "
        f'body={{"booking_id":{booking_id},"action":"approve","token":"{BOOKING_APPROVE_TOKEN}"}}\n'
        f"  deny: same with action=deny"
    )
    _appa_notify(msg)

    return jsonify({
        "status":     "pending_approval",
        "booking_id": booking_id,
        "message":    f"Booking #{booking_id} received — awaiting approval. Poll /api/booking-status/{booking_id}",
    }), 202


@app.route("/api/booking-approve", methods=["POST", "OPTIONS"])
def api_booking_approve():
    """Approve or deny a pending booking. Token-gated."""
    if request.method == "OPTIONS":
        return "", 204

    import json as _json, subprocess, sys
    data       = request.get_json(force=True, silent=True) or {}
    token      = data.get("token", "")
    booking_id = data.get("booking_id")
    action     = data.get("action", "").lower()

    if token != BOOKING_APPROVE_TOKEN:
        return jsonify({"error": "unauthorized"}), 403
    if not booking_id or action not in ("approve", "deny"):
        return jsonify({"error": "booking_id and action (approve|deny) required"}), 400

    conn = _get_db()
    row  = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Booking not found"}), 404

    stored = {}
    try:
        stored = _json.loads(row["aeroplan_ref"] or "{}")
    except Exception:
        pass

    if action == "deny":
        conn.execute("UPDATE bookings SET status='denied' WHERE id=?", (booking_id,))
        conn.commit()
        conn.close()
        _appa_notify(f"\u274c Booking #{booking_id} denied. No action taken.")
        return jsonify({"ok": True, "status": "denied", "booking_id": booking_id})

    # Approve — launch book_alaska.py
    conn.execute("UPDATE bookings SET status='approved' WHERE id=?", (booking_id,))
    conn.commit()
    conn.close()

    flight = stored.get("flight", {})
    client = stored.get("client", {})
    script = os.path.join(os.path.dirname(__file__), "book_alaska.py")
    cmd = [
        sys.executable, script,
        "--origin",     flight.get("origin", ""),
        "--dest",       flight.get("destination", ""),
        "--date",       flight.get("date", ""),
        "--first",      client.get("first_name", ""),
        "--last",       client.get("last_name", ""),
        "--dob",        client.get("dob", ""),
        "--cabin",      flight.get("cabin", "business"),
        "--card",       client.get("card_last4", "2002"),
        "--booking-id", str(booking_id),
    ]
    subprocess.Popen(cmd)
    _appa_notify(f"\u2705 Booking #{booking_id} approved \u2014 launching booking agent.")
    return jsonify({"ok": True, "status": "approved", "booking_id": booking_id})


@app.route("/api/kill", methods=["POST", "OPTIONS"])
def api_kill():
    """Kill switch \u2014 stop all booking processes. Token-gated."""
    if request.method == "OPTIONS":
        return "", 204

    import subprocess
    data = request.get_json(force=True, silent=True) or {}
    if data.get("token") != KILL_TOKEN:
        return jsonify({"error": "unauthorized"}), 403

    result = subprocess.run(["pkill", "-f", "book_alaska.py"], capture_output=True)
    conn = _get_db()
    conn.execute("UPDATE bookings SET status='cancelled' WHERE status IN ('pending_approval','approved')")
    conn.commit()
    conn.close()
    _appa_notify("\U0001f6d1 KILL SWITCH activated. All booking processes stopped.")
    return jsonify({"ok": True, "killed": result.returncode == 0})


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8787))
    print(f"✈️  Flight Search API v2.0  →  http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
