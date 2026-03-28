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

SEATS_AERO_KEY = "pro_34HzjB9LzH46xVzkZz0HeJWekiv"
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(_fetch, to_fetch))


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


# ── seats.aero helpers ─────────────────────────────────────────────────────────
def curl_get(url):
    try:
        resp = _requests.get(url, headers={
            "Partner-Authorization": SEATS_AERO_KEY,
            "accept": "application/json",
        }, timeout=30)
        return resp.json()
    except Exception:
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


def search_seats_aero(origins, destinations, date_from, date_to, cabins, programs):
    origin_str  = ",".join(origins) if isinstance(origins, list) else origins
    dest_str    = ",".join(destinations) if isinstance(destinations, list) else destinations
    cabin_str   = ",".join(cabins) if isinstance(cabins, list) else cabins
    sources_str = ",".join(programs) if programs else ",".join(BUY_MILES_INFO.keys())

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
    promo_rate = info.get("promo_cpp_usd", rate * 0.65)
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

        taxes_usd  = taxes_to_usd(row.get(f"{prefix}TotalTaxesRaw", 0), currency)
        buy_usd    = (miles * rate) / 100
        buy_promo  = (miles * promo_rate) / 100
        total_usd  = buy_usd + taxes_usd
        total_promo = buy_promo + taxes_usd

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

CURRENT_PROMO_CPP = {
    "aeroplan":       1.44,
    "alaska":         1.98,
    "american":       2.26,
    "virginatlantic": 1.47,
    "delta":          2.50,
    "flyingblue":     2.00,
    "united":         2.00,
    "singapore":      1.80,
    "etihad":         1.80,
    "lufthansa":      1.70,
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

_discover_cache = {"tiles": [], "ts": 0.0}
DISCOVER_TTL = 3600  # 1 hour


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
            total         = buy_promo + taxes_usd
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

    pinned   = sorted([c for c in candidates if c.get("_pinned")],      key=lambda x: x["_ratio"], reverse=True)
    unpinned = sorted([c for c in candidates if not c.get("_pinned")],  key=lambda x: x["_ratio"], reverse=True)
    sorted_tiles = pinned + unpinned[:max(0, 20 - len(pinned))]

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


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "flight-search-api", "version": "2.0"})


@app.route("/api/discover")
def api_discover():
    now = time.time()
    if _discover_cache["tiles"] and (now - _discover_cache["ts"]) < DISCOVER_TTL:
        return jsonify({"tiles": _discover_cache["tiles"]})
    tiles = build_discover_tiles()
    _discover_cache["tiles"] = tiles
    _discover_cache["ts"]    = now
    return jsonify({"tiles": tiles})


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
    rows       = search_seats_aero(origins, destinations, date_from, date_to, cabins_str, programs)

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

    combos_list = list(combos)[:40]
    if combos_list:
        print(f"[search] fetching {len(combos_list)} cash prices from Google Flights...")
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

    deals.sort(key=lambda x: (not x["direct"], x["arb_price_usd"]))

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


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8787))
    print(f"✈️  Flight Search API v2.0  →  http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
