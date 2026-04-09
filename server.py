#!/usr/bin/env python3
"""
Award Flight Search API
Wraps seats.aero partner API with scoring, trip details, and buy-miles info.
Run: python3 server.py
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json, subprocess, urllib.parse, time, re, concurrent.futures, asyncio, os

from dotenv import load_dotenv
load_dotenv()

from fast_flights import FlightData, Passengers, get_flights as gf_get_flights

# ── seats.aero key rotation ──────────────────────────────────────────────────
# Keys are tried in order; on 429 the active key is advanced and Appa is notified.
SEATS_AERO_KEYS = [
    os.environ.get("SEATS_AERO_KEY",  "pro_3C3BUt7QiVMzBPN3fwxfU7UCqYc"),  # key 4 (primary)
    os.environ.get("SEATS_AERO_KEY2", "pro_3C0ux62nhQbmMyfWjY7yhmDaQW3"),   # key 1
    os.environ.get("SEATS_AERO_KEY3", "pro_3C1rKk9tlTB4RAddENHbhkUKqN3"),   # key 2
    os.environ.get("SEATS_AERO_KEY4", "pro_34HzjB9LzH46xVzkZz0HeJWekiv"),   # key 3
]
_active_key_index = 0  # mutable index into SEATS_AERO_KEYS

def _active_key():
    return SEATS_AERO_KEYS[_active_key_index]

def _rotate_key():
    """Advance to the next key and notify Appa via hook. Returns new key or None if exhausted."""
    global _active_key_index
    old_index = _active_key_index
    old_key   = SEATS_AERO_KEYS[old_index]
    next_index = old_index + 1
    if next_index >= len(SEATS_AERO_KEYS):
        _notify_appa(f"\u26a0\ufe0f seats.aero ALL KEYS EXHAUSTED (rate-limited). Key {old_index+1}/{len(SEATS_AERO_KEYS)} was last. Manual action needed.")
        return None
    _active_key_index = next_index
    new_key = SEATS_AERO_KEYS[next_index]
    _notify_appa(
        f"\u26a0\ufe0f seats.aero key rotated: key {old_index+1} ({old_key[:12]}...) hit rate limit. "
        f"Now using key {next_index+1}/{len(SEATS_AERO_KEYS)} ({new_key[:12]}...). "
        f"{len(SEATS_AERO_KEYS) - next_index - 1} fallback(s) remaining."
    )
    return new_key

def _notify_appa(text: str):
    """POST a wake event to Appa's hook so it can relay alerts to Telegram."""
    hook_url  = os.environ.get("APPA_HOOK_URL", "https://hooks.airbitrage.io/hooks/wake")
    hook_token = os.environ.get("APPA_HOOK_TOKEN", "flightdash-hook-token-2026")
    payload = json.dumps({"text": text}).encode()
    try:
        import urllib.request
        req = urllib.request.Request(
            hook_url,
            data=payload,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {hook_token}"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"[notify_appa] failed: {e}")

# Legacy single-key alias (used in a few places below)
SEATS_AERO_KEY = property(lambda self: _active_key())  # noqa – overridden per-call
BASE_URL        = "https://seats.aero/partnerapi/search"
TRIPS_URL       = "https://seats.aero/partnerapi/trips"

# ── Buy-miles data ─────────────────────────────────────────────────────────────
BUY_MILES_INFO = {
    "aeroplan": {
        "program_name":   "Air Canada Aeroplan",
        "logo_url":       "https://logo.clearbit.com/aeroplan.com",
        "buy_url":        "https://www.aeroplan.com/buy-miles",
        "currency":       "CAD",
        "standard_cpp":   3.50,   # cents per point in CAD
        "standard_cpp_usd": 2.43,
        "typical_promo_bonus": 50,  # % bonus typically seen
        "promo_cpp_usd":  1.62,   # at 50% bonus
        "min_purchase":   1000,
        "max_purchase":   150000,
        "notes": "Promos (50-100% bonus) run several times per year. Best to wait for one.",
    },
    "alaska": {
        "program_name":   "Alaska Mileage Plan",
        "logo_url":       "https://logo.clearbit.com/alaskaair.com",
        "buy_url":        "https://www.alaskaair.com/content/mileage-plan/ways-to-earn/buy-miles",
        "currency":       "USD",
        "standard_cpp":   2.50,
        "standard_cpp_usd": 2.50,
        "typical_promo_bonus": 60,
        "promo_cpp_usd":  1.56,
        "min_purchase":   1000,
        "max_purchase":   150000,
        "notes": "Frequent 40-100% bonus promos. Never buy at standard rate.",
    },
    "american": {
        "program_name":   "American AAdvantage",
        "logo_url":       "https://logo.clearbit.com/aa.com",
        "buy_url":        "https://www.aa.com/i18n/aadvantage-program/miles/buy-miles.jsp",
        "currency":       "USD",
        "standard_cpp":   2.50,
        "standard_cpp_usd": 2.50,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":  1.79,
        "min_purchase":   1000,
        "max_purchase":   150000,
        "notes": "Promos usually 35-100% bonus. Check aa.com before buying.",
    },
    "virginatlantic": {
        "program_name":   "Virgin Atlantic Flying Club",
        "logo_url":       "https://logo.clearbit.com/virginatlantic.com",
        "buy_url":        "https://www.virginatlantic.com/us/en/flying-club/points/buy-points.html",
        "currency":       "USD",
        "standard_cpp":   2.50,
        "standard_cpp_usd": 2.50,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":  1.79,
        "min_purchase":   500,
        "max_purchase":   100000,
        "notes": "Regular bonus offers. Good for ANA and Delta redemptions.",
    },
    "delta": {
        "program_name":   "Delta SkyMiles",
        "logo_url":       "https://logo.clearbit.com/delta.com",
        "buy_url":        "https://www.delta.com/us/en/skymiles/buy-gift-transfer-miles/buy-miles",
        "currency":       "USD",
        "standard_cpp":   3.50,
        "standard_cpp_usd": 3.50,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":  2.50,
        "min_purchase":   1000,
        "max_purchase":   150000,
        "notes": "Delta miles are dynamic priced — value varies. Only buy for specific high-value routes.",
    },
    "united": {
        "program_name":   "United MileagePlus",
        "logo_url":       "https://logo.clearbit.com/united.com",
        "buy_url":        "https://www.united.com/en/us/fly/mileageplus/miles/buy.html",
        "currency":       "USD",
        "standard_cpp":   3.50,
        "standard_cpp_usd": 3.50,
        "typical_promo_bonus": 100,
        "promo_cpp_usd":  1.75,
        "min_purchase":   1000,
        "max_purchase":   150000,
        "notes": "Occasional 100% bonus sales drop the price significantly.",
    },
    "flyingblue": {
        "program_name":   "Air France/KLM Flying Blue",
        "logo_url":       "https://logo.clearbit.com/flyingblue.com",
        "buy_url":        "https://flyingblue.com/buy-miles",
        "currency":       "EUR",
        "standard_cpp":   2.80,
        "standard_cpp_usd": 2.00,
        "typical_promo_bonus": 40,
        "promo_cpp_usd":  1.43,
        "min_purchase":   2000,
        "max_purchase":   200000,
        "notes": "Monthly Promo Awards offer 25-50% off redemptions — better than buying miles.",
    },
    "etihad": {
        "program_name":   "Etihad Guest",
        "logo_url":       "https://logo.clearbit.com/etihad.com",
        "buy_url":        "https://www.etihad.com/en-us/etihad-guest/miles/buy-miles",
        "currency":       "USD",
        "standard_cpp":   2.80,
        "standard_cpp_usd": 2.80,
        "typical_promo_bonus": 35,
        "promo_cpp_usd":  2.07,
        "min_purchase":   1000,
        "max_purchase":   100000,
        "notes": "Promos offered periodically. Good for Etihad First Apartment redemptions.",
    },
    "singapore": {
        "program_name":   "Singapore KrisFlyer",
        "logo_url":       "https://logo.clearbit.com/singaporeair.com",
        "buy_url":        "https://www.singaporeair.com/en_UK/us/ppsclub-krisflyer/krisflyer/buy-miles/",
        "currency":       "USD",
        "standard_cpp":   2.50,
        "standard_cpp_usd": 2.50,
        "typical_promo_bonus": 30,
        "promo_cpp_usd":  1.92,
        "min_purchase":   1000,
        "max_purchase":   100000,
        "notes": "Good for Singapore Suites (First). Prices vary; watch for promotions.",
    },
    "lufthansa": {
        "program_name":   "Lufthansa Miles & More",
        "logo_url":       "https://logo.clearbit.com/miles-and-more.com",
        "buy_url":        "https://www.miles-and-more.com/row/en/earn/buy-miles.html",
        "currency":       "EUR",
        "standard_cpp":   2.80,
        "standard_cpp_usd": 1.70,
        "typical_promo_bonus": 35,
        "promo_cpp_usd":  1.26,
        "min_purchase":   2000,
        "max_purchase":   100000,
        "notes": "Infrequent promos. Best used for Star Alliance premium cabin sweet spots.",
    },
}

PROGRAM_NAMES = {k: v["program_name"] for k, v in BUY_MILES_INFO.items()}
TAX_FX = {"CAD": 0.694, "EUR": 1.08, "USD": 1.0, "AUD": 0.63, "GBP": 1.27}

# IATA carrier code → logo URL (clearbit by airline domain)
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
_cash_price_cache = {}   # (origin, dest, date, cabin, airlines_key, direct) -> (price_usd | None, timestamp)
CASH_PRICE_TTL    = 6 * 3600  # 6 hours

_GF_SEAT_MAP = {
    "economy":  "economy",
    "premium":  "premium-economy",
    "business": "business",
    "first":    "first",
}

# IATA carrier code → name(s) as Google Flights spells them
_IATA_TO_GF_NAME = {
    "AA": {"American"},
    "AC": {"Air Canada"},
    "AF": {"Air France"},
    "AI": {"Air India"},
    "AK": {"AirAsia"},
    "AMS": {"Amelia"},
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
    "HEL": {"Helsinki Airways"},
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
    "RO": {"TAROM"},
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


# Airlines to exclude from cash price comparisons — budget/low-cost carriers
# that skew the savings calculation (a $200 Norse fare isn't comparable to a
# $3,000 Lufthansa First award).
_EXCLUDED_CARRIER_NAMES = {
    "norse", "norse atlantic",
    "frontier",
    "icelandair",
    "azores", "sata",
}

def _is_excluded_carrier(gf_name: str) -> bool:
    """Return True if this Google Flights airline name should be excluded from cash price results."""
    name_lower = gf_name.lower()
    return any(excl in name_lower for excl in _EXCLUDED_CARRIER_NAMES)

def _airlines_match(gf_name, iata_codes):
    """Return True if gf_name matches any of the IATA codes."""
    gf_lower = gf_name.lower()
    for code in iata_codes:
        names = _IATA_TO_GF_NAME.get(code.upper(), set())
        for n in names:
            if n.lower() in gf_lower or gf_lower in n.lower():
                return True
    return False


def fetch_cash_price(origin, dest, date, cabin, airlines=None, direct=None):
    """
    Return the lowest one-way cash price (USD) for this route/date/cabin from
    Google Flights via fast-flights.

    airlines: list of IATA codes to filter to (e.g. ["AA", "BA"]).
              If None, returns cheapest regardless of airline.
    direct:   if True, only nonstop flights; if False/None, any stops.

    Results are cached for CASH_PRICE_TTL seconds.
    Returns None if the lookup fails or no matching flights found.
    """
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
            # Filter by stops
            if direct and f.stops != 0:
                continue
            # Skip excluded budget/low-cost carriers
            if _is_excluded_carrier(f.name):
                continue
            # Filter by airline
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
    """
    Fetch real cash prices for an iterable of
    (origin, dest, date, cabin, airlines_list_or_None, direct_bool) tuples
    concurrently.  Populates _cash_price_cache in place.
    """
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
    """
    Constructs a Google Flights URL pre-filtered for one-way, cabin class, and optionally nonstop.
    Uses the q= natural language param which Google parses reliably.
    """
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
    """Kayak URL — more reliable nonstop/cabin filtering than Google Flights q= param."""
    cabin_map = {"economy": "e", "premium": "w", "business": "b", "first": "f"}
    c = cabin_map.get(cabin, "b")
    base = f"https://www.kayak.com/flights/{origin}-{destination}/{date}/1adults/{c}"
    if direct:
        base += "?fs=stops=0"
    return base


# ── seats.aero helpers ─────────────────────────────────────────────────────────
def curl_get(url):
    """Fetch a seats.aero URL, rotating the API key on 429."""
    for attempt in range(len(SEATS_AERO_KEYS)):
        key = _active_key()
        result = subprocess.run(
            ["curl", "-s", "--max-time", "30", "-w", "\n%{http_code}", url,
             "-H", f"Partner-Authorization: {key}",
             "-H", "accept: application/json"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return None
        # Last line is the HTTP status code
        lines = result.stdout.rsplit("\n", 1)
        body  = lines[0]
        status = lines[1].strip() if len(lines) > 1 else "0"
        if status == "429":
            print(f"[seats.aero] 429 rate-limit on key {_active_key_index+1}, rotating...")
            rotated = _rotate_key()
            if rotated is None:
                return None  # all keys exhausted
            continue
        try:
            return json.loads(body)
        except Exception:
            return None
    return None


def fetch_trips(availability_id, direct_only=False, carriers_filter=None):
    data = curl_get(f"{TRIPS_URL}/{availability_id}")
    if not data:
        return []
    trips = data if isinstance(data, list) else data.get("data", [])

    # Parse carrier filter into a set of 2-letter codes
    filter_codes = set()
    if carriers_filter:
        for c in carriers_filter.replace(" ", "").split(","):
            if c:
                filter_codes.add(c.upper())

    results = []
    for trip in trips:
        stops = trip.get("Stops", 0)
        carriers = trip.get("Carriers", "")
        carrier_codes = set(c.strip().upper() for c in carriers.split(",") if c.strip())

        # Apply direct_only filter
        if direct_only and stops != 0:
            continue

        # Apply carrier filter — trip must involve at least one of the requested carriers
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
            "cabin":              normalize_cabin(trip.get("Cabin", "")),
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


# Normalize seats.aero cabin codes to standard frontend values
_CABIN_NORM = {
    "first":    "first",   "f": "first",
    "business": "business", "j": "business", "c": "business", "d": "business", "i": "business",
    "premium":  "premium",  "w": "premium",  "s": "premium",
    "economy":  "economy",  "y": "economy",  "m": "economy",  "b": "economy",
                             "h": "economy",  "k": "economy",  "l": "economy",
                             "q": "economy",  "t": "economy",  "v": "economy",
                             "x": "economy",  "n": "economy",
}

def normalize_cabin(cabin: str) -> str:
    if not cabin:
        return "economy"
    return _CABIN_NORM.get(cabin.lower(), cabin.lower())


# Programs we support buying miles for and will show results for.
# Any seats.aero source not in this list is excluded from search results.
ALLOWED_PROGRAMS = {"aeroplan", "alaska", "american", "virginatlantic", "flyingblue"}


def search_seats_aero(origins, destinations, date_from, date_to, cabins, programs):
    origin_str  = ",".join(origins) if isinstance(origins, list) else origins
    dest_str    = ",".join(destinations) if isinstance(destinations, list) else destinations
    cabin_str   = ",".join(cabins) if isinstance(cabins, list) else cabins
    # If caller passes programs, intersect with allowed list; otherwise use all allowed
    if programs:
        effective = [p for p in programs if p in ALLOWED_PROGRAMS]
    else:
        effective = list(ALLOWED_PROGRAMS)
    sources_str = ",".join(effective) if effective else ",".join(ALLOWED_PROGRAMS)

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
    """
    Score a seats.aero row for all applicable cabins.

    cabin_pref controls which cabins are returned:
      - "economy"  → economy only
      - "business" → business + economy (economy always shown as upsell/context)
      - "first"    → first + business + economy
      - "premium"  → premium + economy
      - "any"/None → all four cabins

    Returns a list of scored deal dicts (may be empty). Callers must flatten.
    """
    source     = row.get("Source", "")
    info       = BUY_MILES_INFO.get(source, {})
    rate       = info.get("standard_cpp_usd", 2.0)
    # Always use CURRENT_PROMO_CPP as the single source of truth for displayed price
    promo_rate = CURRENT_PROMO_CPP.get(source, info.get("promo_cpp_usd", rate * 0.65))
    route      = row.get("Route", {})
    currency   = row.get("TaxesCurrency", "USD")
    distance   = route.get("Distance", 0)

    # Determine which cabins to score.
    # For "business" searches we always also include economy so users see the
    # cheaper option without having to run a separate search.
    CABIN_ORDER = [("first","F"), ("business","J"), ("premium","W"), ("economy","Y")]
    cabin_sets = {
        "first":    [("first","F"), ("business","J"), ("premium","W"), ("economy","Y")],
        "business": [("business","J"), ("economy","Y")],
        "premium":  [("premium","W"), ("economy","Y")],
        "economy":  [("economy","Y")],
    }
    if cabin_pref and cabin_pref != "any":
        check = cabin_sets.get(cabin_pref, [("business","J"), ("economy","Y")])
    else:
        check = CABIN_ORDER

    results = []
    for cabin_name, prefix in check:
        if not row.get(f"{prefix}Available"):
            continue
        if int(row.get(f"{prefix}RemainingSeats") or 0) <= 0:
            continue
        miles = int(row.get(f"{prefix}MileageCost") or 0)
        if miles <= 0:
            continue

        taxes_usd   = taxes_to_usd(row.get(f"{prefix}TotalTaxesRaw", 0), currency)
        buy_usd     = (miles * rate) / 100
        buy_promo   = (miles * promo_rate) / 100
        total_usd   = buy_usd + taxes_usd
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

        savings = round(cash_est - total_usd, 0) if cash_est else None
        ratio   = round(cash_est / total_usd, 2)  if (cash_est and total_usd > 0) else None

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

        carrier_logos = {code: CARRIER_LOGOS[code] for code in airline_codes if code in CARRIER_LOGOS}

        results.append({
            "availability_id":          row.get("ID", ""),
            "date":                     row.get("Date", ""),
            "origin":                   orig_code,
            "destination":              dest_code,
            "distance_miles":           distance,
            "program":                  source,
            "program_name":             info.get("program_name", source),
            "cabin":                    cabin_name,
            "miles":                    miles,
            "taxes_usd":                round(taxes_usd, 2),
            "arb_miles_cost_usd":       round(buy_usd, 2),
            "arb_miles_cost_promo_usd": round(buy_promo, 2),
            "arb_price_usd":            round(total_usd, 2),
            "arb_price_promo_usd":      round(total_promo, 2),
            "cash_price_usd":           cash_est if cash_est else None,
            "cash_price_source":        cash_est_source,
            "savings_usd":              savings,
            "value_ratio":              ratio,
            "airlines":                 row.get(f"{prefix}AirlinesRaw", ""),
            "carrier_logos":            carrier_logos,
            "program_logo_url":         info.get("logo_url", ""),
            "direct":                   is_direct,
            "remaining_seats":          row.get(f"{prefix}RemainingSeats", 0),
            "taxes_currency":           currency,
            "buy_miles_info":           buy_info,
            "google_flights_url":       google_flights_url_simple(
                orig_code, dest_code, date_str, cabin_name, is_direct),
            "kayak_url":                kayak_url(
                orig_code, dest_code, date_str, cabin_name, is_direct),
        })
    return results


# ── Discover endpoint data ────────────────────────────────────────────────────

CITY_NAMES = {
    "JFK":"New York","LAX":"Los Angeles","SFO":"San Francisco","BOS":"Boston",
    "ORD":"Chicago","MIA":"Miami","YYZ":"Toronto","YVR":"Vancouver","YUL":"Montreal",
    "LHR":"London","CDG":"Paris","FRA":"Frankfurt","AMS":"Amsterdam","ZRH":"Zurich",
    "FCO":"Rome","MAD":"Madrid","LIS":"Lisbon","ARN":"Stockholm","VIE":"Vienna",
    "MUC":"Munich","BCN":"Barcelona","HEL":"Helsinki","CPH":"Copenhagen",
    "NRT":"Tokyo","HND":"Tokyo","ICN":"Seoul","HKG":"Hong Kong","PVG":"Shanghai",
    "BKK":"Bangkok","SIN":"Singapore","KUL":"Kuala Lumpur",
    "SYD":"Sydney","MEL":"Melbourne","AKL":"Auckland",
    "DXB":"Dubai","DOH":"Doha","AUH":"Abu Dhabi","RUH":"Riyadh","AMM":"Amman","CAI":"Cairo",
    "DEL":"Delhi","BOM":"Mumbai",
    "JNB":"Johannesburg","CPT":"Cape Town","ADD":"Addis Ababa","NBO":"Nairobi",
    "GRU":"São Paulo","EZE":"Buenos Aires","BOG":"Bogotá","MEX":"Mexico City",
    "LIM":"Lima","SCL":"Santiago",
}

REGION_MAP = {
    "LHR":"Europe","CDG":"Europe","FRA":"Europe","AMS":"Europe","ZRH":"Europe",
    "FCO":"Europe","MAD":"Europe","LIS":"Europe","ARN":"Europe","VIE":"Europe",
    "MUC":"Europe","BCN":"Europe","HEL":"Europe","CPH":"Europe",
    "NRT":"Asia","HND":"Asia","ICN":"Asia","HKG":"Asia","PVG":"Asia",
    "BKK":"Asia","SIN":"Asia","KUL":"Asia",
    "SYD":"Pacific","MEL":"Pacific","AKL":"Pacific",
    "DXB":"Middle East","DOH":"Middle East","AUH":"Middle East",
    "RUH":"Middle East","AMM":"Middle East",
    "CAI":"Africa","JNB":"Africa","CPT":"Africa","ADD":"Africa","NBO":"Africa",
    "DEL":"South Asia","BOM":"South Asia",
    "GRU":"Latin Am.","EZE":"Latin Am.","BOG":"Latin Am.",
    "MEX":"Latin Am.","LIM":"Latin Am.","SCL":"Latin Am.",
}

# Current promo buy rates (cents per mile) — update when promos change
# ⚠️ Vault actual costs (Apr 3 2026): Aeroplan=1.49¢, Alaska=1.88125¢, Virgin=1.18¢
# Cents-per-point rates used for ALL price display (search + discover).
# Pulled from vault_accounts.cost_per_point_cents — actual cost paid per program.
# Fallback values used for programs not in the vault.
def _load_cpp_from_vault():
    import sqlite3, os
    defaults = {
        "aeroplan":       1.49,
        "alaska":         1.88125,
        "virginatlantic": 1.18,
        "american":       2.26,
        "flyingblue":     2.00,
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

# Route batches to scan for discover tiles (broad sweeps)
DISCOVER_SEARCHES = [
    # US/Canada → Europe  (Aeroplan sweet spot + Alaska BA)
    ("JFK,EWR,BOS,ORD,MIA,LAX,SFO,YYZ", "LHR,CDG,FRA,ZRH,AMS,FCO",
     "aeroplan,alaska,american,virginatlantic", "business,first"),
    # Asia → Europe  (SIN/NRT/ICN/DEL/BOM → Europe)
    ("SIN,HKG,NRT,ICN,BKK,DEL,BOM", "LHR,CDG,FRA,ZRH,AMS,FCO",
     "aeroplan,alaska,american,flyingblue", "business,first"),
    # Tokyo outbound  (JL First sweet spot)
    ("HND,NRT", "BKK,SIN,PVG,ICN,HKG,SYD,LAX,JFK,ORD,LHR",
     "american,aeroplan,alaska", "business,first"),
    # Middle East → Europe
    ("DXB,DOH,AUH,RUH,CAI,AMM", "LHR,CDG,FRA,ZRH,FCO,ARN",
     "alaska,aeroplan,american,flyingblue", "business"),
    # Intra-Asia
    ("ICN,HKG,NRT,SIN,BKK,DEL", "NRT,SIN,HKG,BKK,SYD,MEL",
     "alaska,aeroplan,american", "business,first"),
    # US → Asia/Pacific
    ("JFK,LAX,MIA,ORD,SFO,BOS", "NRT,SIN,ICN,HKG,SYD,MEL,BKK",
     "american,aeroplan,alaska", "business,first"),
    # Tokyo → Shanghai (intra-Asia highlight)
    ("NRT,HND", "PVG,SHA",
     "aeroplan,alaska,american", "business,first"),
]

# Pinned routes — always included in discover if availability exists.
# date_override: "today" = force search on today only; None = 90-day window.
# Each entry: (origin, dest, sources, cabins, date_override)
PINNED_SEARCHES = [
    ("BOS", "ZRH", "aeroplan", "business,first", None),        # BOS→ZRH Swiss via Aeroplan
    ("EWR", "FRA", "aeroplan", "first",          "today"),     # EWR→FRA Lufthansa First today only
    ("SIN", "LHR", "aeroplan", "business",       None),        # SIN→LHR SQ business via Aeroplan
]

_discover_cache = {"tiles": [], "ts": 0.0}
# Discover is refreshed once daily via cron (POST /api/discover/refresh).
# TTL is a fallback safety net — not the primary refresh mechanism.
# Set DISCOVER_TTL_SECONDS=86400 on Railway.
DISCOVER_TTL = int(os.environ.get("DISCOVER_TTL_SECONDS", 86400))
DISCOVER_CACHE_FILE = os.environ.get("DISCOVER_CACHE_FILE", "/tmp/discover_cache.json")
DISCOVER_REFRESH_TOKEN = os.environ.get("DISCOVER_REFRESH_TOKEN", "discover-refresh-token-2026")
BOOKING_APPROVE_TOKEN = os.environ.get("BOOKING_APPROVE_TOKEN", "booking-approve-token-2026")
KILL_TOKEN             = os.environ.get("KILL_TOKEN", "kill-switch-token-2026")

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
        pass  # no cache file yet, that's fine

def _save_discover_cache_to_disk(tiles):
    """Persist discover cache to disk so Railway restarts don't burn quota."""
    try:
        with open(DISCOVER_CACHE_FILE, "w") as f:
            json.dump({"tiles": tiles, "ts": time.time()}, f)
        print(f"[discover] saved {len(tiles)} tiles to disk cache")
    except Exception as e:
        print(f"[discover] warning: could not save cache to disk: {e}")


def build_discover_tiles():
    import datetime
    today = datetime.date.today()
    start = today.strftime("%Y-%m-%d")
    end   = (today + datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")

    all_rows = []

    # ── Broad sweeps ─────────────────────────────────────────────────────────
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
            rows = data.get("data", []) if isinstance(data, dict) else data
            all_rows.extend(rows)
        time.sleep(0.3)

    # ── Pinned routes — fetched specifically, tagged so they survive the cut ─
    pinned_ids = set()
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
                pinned_ids.add(r.get("ID", ""))
            all_rows.extend(rows)
        time.sleep(0.3)

    # ── Filter to ALLOWED_PROGRAMS only ─────────────────────────────────────────
    all_rows = [r for r in all_rows if r.get("Source", "") in ALLOWED_PROGRAMS]

    # ── First pass: find the best (lowest miles) candidate per key ──────────────
    best = {}
    for row in all_rows:
        source   = row.get("Source", "")
        route    = row.get("Route", {})
        promo_cpp = CURRENT_PROMO_CPP.get(source, 2.0)
        currency = row.get("TaxesCurrency", "USD")

        for cabin_name, prefix in [("first", "F"), ("business", "J")]:
            if not row.get(f"{prefix}Available"):
                continue
            if int(row.get(f"{prefix}RemainingSeats") or 0) <= 0:
                continue
            miles = int(row.get(f"{prefix}MileageCost") or 0)
            if miles <= 0:
                continue

            taxes_usd  = taxes_to_usd(row.get(f"{prefix}TotalTaxesRaw", 0), currency)
            buy_promo  = (miles * promo_cpp) / 100
            total      = buy_promo + taxes_usd

            orig = route.get("OriginAirport", "?")
            dest = route.get("DestinationAirport", "?")
            date = row.get("Date", "")
            key  = (orig, dest, source, cabin_name)

            airline_codes = [a.strip() for a in (row.get(f"{prefix}AirlinesRaw") or "").split(",") if a.strip()]
            is_direct     = bool(row.get(f"{prefix}Direct", False))

            is_pinned = bool(row.get("_pinned"))

            # Track by lowest total cost (will re-rank by ratio once we have cash prices)
            # A pinned row always wins its key slot
            if key not in best or total < best[key]["_total"] or (is_pinned and not best[key].get("_pinned")):
                best[key] = {
                    "origin_code":       orig,
                    "origin_city":       CITY_NAMES.get(orig, orig),
                    "destination_code":  dest,
                    "destination_city":  CITY_NAMES.get(dest, dest),
                    "region":            REGION_MAP.get(dest, "Intl"),
                    "date":              date,
                    "cabin":             cabin_name,
                    "miles":             miles,
                    "taxes_usd":         round(taxes_usd),
                    "arb_miles_cost_promo_usd": round(buy_promo),
                    "arb_price_promo_usd":      round(total),
                    "program":           source,
                    "program_name":      BUY_MILES_INFO.get(source, {}).get("program_name", source),
                    "direct":            is_direct,
                    "airlines":          airline_codes,
                    "remaining_seats":   row.get(f"{prefix}RemainingSeats", 0),
                    "availability_exists": True,
                    "availability_id":   row.get("ID", ""),
                    "_total":            total,
                    "_airlines":         airline_codes,
                    "_pinned":           is_pinned,
                }

    candidates = list(best.values())

    # ── Prefetch real cash prices from Google Flights ─────────────────────────
    combos = set()
    for t in candidates:
        airlines     = t.get("_airlines") or None
        airlines_key = tuple(sorted(a.upper() for a in airlines)) if airlines else None
        combos.add((t["origin_code"], t["destination_code"], t["date"], t["cabin"],
                    airlines_key, t["direct"]))
    print(f"[discover] fetching {len(combos)} cash prices from Google Flights...")
    prefetch_cash_prices(combos)

    # ── Second pass: score by value ratio using real cash prices ─────────────
    for t in candidates:
        orig, dest, date, cabin = t["origin_code"], t["destination_code"], t["date"], t["cabin"]
        airlines     = t.get("_airlines") or None
        airlines_key = tuple(sorted(a.upper() for a in airlines)) if airlines else ()
        cache_key    = (orig, dest, date, cabin, airlines_key, bool(t["direct"]))
        cache_hit    = _cash_price_cache.get(cache_key)
        cash_price   = cache_hit[0] if (cache_hit and cache_hit[0] is not None) else None
        total = t["arb_price_promo_usd"]
        t["cash_price_usd"]  = cash_price
        t["savings_usd"]    = round(cash_price - total, 0) if cash_price else None
        t["value_ratio"]    = round(cash_price / total, 2)  if (cash_price and total > 0) else None
        t["cash_price_source"] = "google_flights" if cash_price else "unavailable"
        t["_ratio"]         = t["value_ratio"] or 0

    # Sort: pinned tiles first (in their own ratio order), then rest by ratio
    pinned   = sorted([c for c in candidates if c.get("_pinned")],  key=lambda x: x["_ratio"], reverse=True)
    unpinned = sorted([c for c in candidates if not c.get("_pinned")], key=lambda x: x["_ratio"], reverse=True)
    # Fill up to 20: pinned always included, unpinned fill remaining slots
    sorted_tiles = pinned + unpinned[:max(0, 20 - len(pinned))]
    for t in sorted_tiles:
        t.pop("_ratio", None)
        t.pop("_total", None)
        t.pop("_airlines", None)
        t.pop("_pinned", None)

    # ── Enrich tiles with trip details from trips API ────────────────────────
    def _enrich_tile(t):
        avail_id = t.get("availability_id", "")
        if not avail_id:
            return
        tile_airlines = set(a.upper() for a in (t.get("airlines") or []))
        is_direct     = t.get("direct", False)

        all_trips = fetch_trips(avail_id)
        if not all_trips:
            return

        # Filter to trips whose carriers match the tile's airline(s)
        matched = [tr for tr in all_trips
                   if tile_airlines and
                   any(c.strip().upper() in tile_airlines
                       for c in tr.get("carriers", "").split(","))]
        # If no match (e.g. codeshare mismatch), fall back to all trips
        if not matched:
            matched = all_trips

        # Among matched, prefer direct if tile says direct; then fewest stops, shortest duration
        if is_direct:
            direct_trips = [tr for tr in matched if tr["stops"] == 0]
            pool_trips = direct_trips if direct_trips else matched
        else:
            pool_trips = matched

        best = sorted(pool_trips, key=lambda x: (x["stops"], x["total_duration_min"]))[0]

        t["departs_at"]    = best.get("departs_at") or None
        t["arrives_at"]    = best.get("arrives_at") or None
        t["stops"]         = best.get("stops", 0)
        t["carriers"]      = best.get("carriers", "")
        t["flight_numbers"] = best.get("flight_numbers", "")
        t["segments"]      = best.get("segments", [])
        # aircraft_name = first long-haul segment (most seats, i.e. last segment for hub connections)
        segs = best.get("segments", [])
        main_seg = segs[-1] if len(segs) > 1 else (segs[0] if segs else None)
        t["aircraft_name"] = main_seg.get("aircraft_name") or None if main_seg else None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(_enrich_tile, sorted_tiles))

    return sorted_tiles


def handle_discover():
    now = time.time()
    if _discover_cache["tiles"] and (now - _discover_cache["ts"]) < DISCOVER_TTL:
        return {"tiles": _discover_cache["tiles"]}, 200
    # Cache is stale/empty but no active refresh — return empty rather than
    # burning API quota. Use POST /api/discover/refresh to populate.
    print("[discover] cache empty/stale, returning empty (use /api/discover/refresh to populate)")
    return {"tiles": _discover_cache["tiles"]}, 200

def handle_discover_refresh(token):
    """Force a discover rebuild. Called by daily cron. Requires token auth."""
    if token != DISCOVER_REFRESH_TOKEN:
        return {"error": "unauthorized"}, 401
    print("[discover] refresh triggered")
    tiles = build_discover_tiles()
    _discover_cache["tiles"] = tiles
    _discover_cache["ts"]    = time.time()
    _save_discover_cache_to_disk(tiles)
    return {"ok": True, "tiles": len(tiles)}, 200


# ── Request handlers ───────────────────────────────────────────────────────────
def _score_rows(rows, cabin_pref):
    """Score a list of raw seats.aero rows, prefetch cash prices, return deduped deals list."""
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
            if row.get(f"{prefix}Available") and int(row.get(f"{prefix}MileageCost") or 0) > 0 and int(row.get(f"{prefix}RemainingSeats") or 0) > 0:
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
        for d in score_row(row, cabin_pref):
            key = (d["origin"], d["destination"], d["program"], d["cabin"], d["date"])
            if key in seen:
                continue
            seen.add(key)
            deals.append(d)

    deals.sort(key=lambda x: (not x["direct"], x["arb_price_usd"]))
    return deals


def _flex_date_search(origins, destinations, date_from, cabin, cabin_api_map, programs,
                      target_price, flex_days=3, price_threshold=1.5):
    """
    Search ±flex_days around date_from. Return deals whose arb_price_usd is
    at least (1/price_threshold) of target_price — i.e. meaningfully cheaper.
    Each returned deal gets alt_date=True.
    Returns (deals_list, flex_date_from, flex_date_to).
    """
    import datetime
    base = datetime.date.fromisoformat(date_from)
    flex_start = (base - datetime.timedelta(days=flex_days)).strftime("%Y-%m-%d")
    flex_end   = (base + datetime.timedelta(days=flex_days)).strftime("%Y-%m-%d")

    cabins_str = cabin_api_map.get(cabin, "business")
    rows = search_seats_aero(origins, destinations, flex_start, flex_end, cabins_str, programs)
    cabin_pref = cabin if cabin != "any" else None
    all_deals = _score_rows(rows, cabin_pref)

    # Exclude the original date (already tried), keep only genuinely cheaper ones
    threshold_price = target_price / price_threshold  # price must be <= this to qualify
    alt_deals = []
    for d in all_deals:
        if d["date"] == date_from:
            continue
        if d["arb_price_usd"] <= threshold_price:
            d["alt_date"] = True
            alt_deals.append(d)

    alt_deals.sort(key=lambda x: (not x["direct"], x["arb_price_usd"]))
    return alt_deals, flex_start, flex_end


def handle_search(body):
    origins      = body.get("origin", [])
    destinations = body.get("destination", [])
    date_from    = body.get("date_from", "")
    date_to      = body.get("date_to", date_from)
    cabin        = body.get("cabin", "business").lower()
    programs     = body.get("programs", [])

    if not origins or not destinations or not date_from:
        return {"error": "Missing required fields: origin, destination, date_from"}, 400

    if isinstance(origins, str):
        origins = [o.strip() for o in origins.split(",")]
    if isinstance(destinations, str):
        destinations = [d.strip() for d in destinations.split(",")]

    cabin_api_map = {
        "economy": "economy", "premium": "premium",
        "premium economy": "premium", "business": "business",
        "first": "first", "any": "economy,premium,business,first",
    }
    # Fallback chain: if no results in requested cabin, step down to cheaper cabins
    CABIN_FALLBACK = {
        "first":    ["first", "business", "premium", "economy"],
        "business": ["business", "premium", "economy"],
        "premium":  ["premium", "economy"],
        "economy":  ["economy"],
        "any":      ["any"],
    }
    cabin_fallback_chain = CABIN_FALLBACK.get(cabin, ["business", "economy"])

    cabin_pref = cabin if cabin != "any" else None
    cabin_fallback_info = None
    deals = []
    effective_cabin = cabin  # the cabin we actually found results for

    for try_cabin in cabin_fallback_chain:
        cabins_str = cabin_api_map.get(try_cabin, try_cabin)
        rows = search_seats_aero(origins, destinations, date_from, date_to, cabins_str, programs)
        cabin_pref_try = try_cabin if try_cabin != "any" else None
        deals = _score_rows(rows, cabin_pref_try)
        # Filter to only deals matching this cabin (score_row may include lower cabins)
        matching = [d for d in deals if d["cabin"] == try_cabin] if try_cabin != "any" else deals
        if matching:
            deals = deals  # keep full list (includes economy context rows)
            effective_cabin = try_cabin
            cabin_pref = cabin_pref_try
            if try_cabin != cabin:
                cabin_label = {"business": "Business", "premium": "Premium Economy",
                               "economy": "Economy", "first": "First"}.get(try_cabin, try_cabin.title())
                orig_label  = {"business": "Business", "premium": "Premium Economy",
                               "economy": "Economy", "first": "First"}.get(cabin, cabin.title())
                cabin_fallback_info = {
                    "reason": "cabin_unavailable",
                    "requested_cabin": cabin,
                    "found_cabin": try_cabin,
                    "message": f"No {orig_label} availability on {date_from}. Showing {cabin_label} options instead.",
                }
            break

    # ── Flex-date fallback ────────────────────────────────────────────────────
    # Case 1: no results at all (even after cabin fallback) → search ±3 days
    # Case 2: results found but cheapest is >50% more expensive than the best
    #         deal within ±3 days → surface those cheaper alt-date options
    flex_info = None
    cabins_str = cabin_api_map.get(effective_cabin, effective_cabin)

    if not deals:
        # No results on any cabin — search ±3 days with full cabin fallback
        import datetime
        base = datetime.date.fromisoformat(date_from)
        flex_start = (base - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
        flex_end   = (base + datetime.timedelta(days=3)).strftime("%Y-%m-%d")
        # Try all cabins in fallback order across the flex window
        all_alt_deals = []
        for try_cabin in cabin_fallback_chain:
            cabins_str_try = cabin_api_map.get(try_cabin, try_cabin)
            rows2 = search_seats_aero(origins, destinations, flex_start, flex_end, cabins_str_try, programs)
            cabin_pref_try = try_cabin if try_cabin != "any" else None
            alt = _score_rows(rows2, cabin_pref_try)
            alt = [d for d in alt if d["date"] != date_from]
            if alt:
                all_alt_deals = alt
                effective_cabin = try_cabin
                cabin_pref = cabin_pref_try
                break
        for d in all_alt_deals:
            d["alt_date"] = True
        all_alt_deals.sort(key=lambda x: (not x["direct"], x["arb_price_usd"]))

        if all_alt_deals:
            flex_info = {
                "reason": "no_results_on_date",
                "searched_date": date_from,
                "flex_range": f"{flex_start} to {flex_end}",
                "message": f"No availability found on {date_from}. Showing best options within ±3 days.",
            }
            if cabin_fallback_info:
                flex_info["message"] += f" ({cabin_fallback_info['message']})"
            deals = all_alt_deals

    else:
        # Results found — check if ±3 days has something >33% cheaper (i.e. target is >150% of best alt)
        best_price = deals[0]["arb_price_usd"]
        alt_deals, flex_start, flex_end = _flex_date_search(
            origins, destinations, date_from, cabin, cabin_api_map, programs,
            target_price=best_price,
            flex_days=3, price_threshold=1.5,
        )
        if alt_deals:
            # Merge alt deals into results (they're already marked alt_date=True)
            existing_keys = {(d["origin"], d["destination"], d["program"], d["cabin"], d["date"]) for d in deals}
            new_alts = [d for d in alt_deals if (d["origin"], d["destination"], d["program"], d["cabin"], d["date"]) not in existing_keys]
            if new_alts:
                best_alt_price = new_alts[0]["arb_price_usd"]
                pct_cheaper = round((best_price - best_alt_price) / best_price * 100)
                flex_info = {
                    "reason": "cheaper_nearby_date",
                    "searched_date": date_from,
                    "flex_range": f"{flex_start} to {flex_end}",
                    "message": f"Flights on {date_from} are available but up to {pct_cheaper}% cheaper on nearby dates. Alt-date options shown below.",
                    "best_price_on_date": best_price,
                    "best_alt_price": best_alt_price,
                }
                deals = deals + new_alts

    # ── Expand deals into individual trip itineraries ────────────────────────
    # Each deal is a route-level result. Fetch its trips and emit one result
    # per itinerary so the UI shows separate cards per flight option.
    expanded = []
    seen_trip_ids = set()
    for deal in deals:
        avail_id = deal.get("availability_id", "")
        requested_cabin = deal.get("cabin", cabin)
        if not avail_id:
            expanded.append(deal)
            continue
        try:
            trips = fetch_trips(avail_id)
        except Exception:
            trips = []

        # Filter trips to only the requested cabin
        cabin_trips = [t for t in trips if (t.get("cabin") or "").lower() == requested_cabin.lower()]
        if not cabin_trips:
            # fallback: show all trips if no cabin match
            cabin_trips = trips

        if not cabin_trips:
            expanded.append(deal)
            continue

        for trip in cabin_trips:
            trip_id = trip.get("trip_id", "")
            if trip_id and trip_id in seen_trip_ids:
                continue
            if trip_id:
                seen_trip_ids.add(trip_id)

            segments = trip.get("segments", [])
            carriers = trip.get("carriers", "")
            carrier_codes = [c.strip() for c in carriers.split(",") if c.strip()] if carriers else []
            flight_numbers = trip.get("flight_numbers", "")
            is_direct = trip.get("stops", 1) == 0
            departs_at = trip.get("departs_at", "")
            arrives_at = trip.get("arrives_at", "")
            remaining = trip.get("remaining_seats", deal.get("remaining_seats", 0))
            trip_miles = trip.get("miles", 0) or deal.get("miles", 0)
            # taxes_raw is in local currency (CAD usually for Aeroplan), convert to USD
            taxes_raw = trip.get("taxes_raw", 0) or 0
            taxes_currency = trip.get("taxes_currency", "USD")
            trip_taxes = round(taxes_to_usd(taxes_raw, taxes_currency), 2) if taxes_raw else deal.get("taxes_usd", 0)

            # Recompute arb price with trip-level miles/taxes
            promo_cpp = CURRENT_PROMO_CPP.get(deal.get("program", ""), 2.0)
            standard_cpp = BUY_MILES_INFO.get(deal.get("program", ""), {}).get("standard_cpp_usd", 2.5)
            buy_promo = round((trip_miles * promo_cpp) / 100, 2) if trip_miles else deal.get("arb_miles_cost_promo_usd", 0)
            buy_standard = round((trip_miles * standard_cpp) / 100, 2) if trip_miles else deal.get("arb_miles_cost_usd", 0)
            arb_price_promo = round(buy_promo + trip_taxes, 2)
            arb_price_standard = round(buy_standard + trip_taxes, 2)

            entry = {**deal}
            entry["availability_id"] = avail_id
            entry["trip_id"] = trip_id or f"{avail_id}-{len(expanded)}"
            entry["miles"] = trip_miles or deal.get("miles", 0)
            entry["taxes_usd"] = trip_taxes or deal.get("taxes_usd", 0)
            entry["arb_miles_cost_promo_usd"] = buy_promo
            entry["arb_price_promo_usd"] = arb_price_promo
            entry["arb_miles_cost_usd"] = buy_standard
            entry["arb_price_usd"] = arb_price_standard
            entry["direct"] = is_direct
            entry["stops"] = trip.get("stops", 0)
            entry["carriers"] = carriers
            entry["airlines"] = carrier_codes
            entry["flight_numbers"] = flight_numbers
            entry["departs_at"] = departs_at
            entry["arrives_at"] = arrives_at
            entry["remaining_seats"] = remaining
            entry["segments"] = segments
            entry["cabin"] = normalize_cabin(trip.get("cabin", requested_cabin))
            expanded.append(entry)

    # Sort: requested cabin first, then by arb price
    def _sort_key(d):
        cabin_match = 0 if d.get("cabin", "").lower() == cabin.lower() else 1
        alt = 1 if d.get("alt_date") else 0
        return (alt, cabin_match, d.get("arb_price_usd", 9999))

    expanded.sort(key=_sort_key)

    summary = None
    if expanded:
        b = next((d for d in expanded if not d.get("alt_date")), expanded[0])
        cash_str = f"~${b['cash_price_usd']:,}" if b.get("cash_price_usd") else "unknown cash price"
        fn_str = f" · {b['flight_numbers']}" if b.get("flight_numbers") else ""
        summary = (
            f"Best deal: {b['date']} · {b['program_name']} · "
            f"{b['miles']:,} miles + ${b['taxes_usd']:.0f} taxes "
            f"(arb price ~${b['arb_price_usd']:.0f} standard, "
            f"~${b['arb_price_promo_usd']:.0f} at promo). "
            f"{'Nonstop.' if b['direct'] else 'Connecting.'}{fn_str} "
            f"Cash price: {cash_str}."
        )

    return {
        "results": expanded[:50],
        "total_found": len(expanded),
        "summary": summary,
        "flex_date_info": flex_info,
        "cabin_fallback_info": cabin_fallback_info,
        "query": {"origins": origins, "destinations": destinations,
                  "date_from": date_from, "date_to": date_to,
                  "cabin": cabin, "programs": programs},
    }, 200


def handle_trips(availability_id, query_string=""):
    if not availability_id:
        return {"error": "Missing availability_id"}, 400
    params = urllib.parse.parse_qs(query_string)
    direct_only     = params.get("direct_only", ["false"])[0].lower() == "true"
    carriers_filter = params.get("carriers", [None])[0]
    trips = fetch_trips(availability_id, direct_only=direct_only, carriers_filter=carriers_filter)
    return {"trips": trips, "count": len(trips)}, 200


# ── Inbound email webhook handler ─────────────────────────────────────────────

def handle_inbound_email(body: dict) -> tuple[dict, int]:
    """
    POST /api/inbound-email
    Mailgun inbound webhook. Parses recipient and body for 6-digit codes.
    Returns 200 always (Mailgun requires this).
    """
    from email_manager import store_inbound_code

    # Mailgun sends form-encoded or JSON depending on configuration.
    # The 'recipient' field is the To: address; 'body-plain' or 'stripped-text'
    # contains the email body.
    recipient = (
        body.get("recipient") or
        body.get("To") or
        body.get("to") or
        ""
    )
    email_body = (
        body.get("body-plain") or
        body.get("stripped-text") or
        body.get("body-html") or
        body.get("message") or
        ""
    )

    # Extract all 6-digit sequences from the body
    codes = re.findall(r"\b(\d{6})\b", email_body)
    if not codes:
        # Also try subject line
        subject = body.get("subject") or body.get("Subject") or ""
        codes = re.findall(r"\b(\d{6})\b", subject)

    if recipient and codes:
        for code in codes:
            try:
                store_inbound_code(recipient, code)
            except Exception as e:
                print(f"[inbound-email] Failed to store code {code} for {recipient}: {e}")
    else:
        print(f"[inbound-email] No 6-digit codes found. recipient={recipient!r}")

    # Always return 200 so Mailgun doesn't retry
    return {"status": "ok", "codes_stored": len(codes)}, 200


# ── Full end-to-end booking handler ───────────────────────────────────────────

def _ping_appa(booking_id: int, flight: dict, client: dict, deal: dict):
    """Launch book_alaska.py to handle the booking automatically."""
    import sys
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
    try:
        subprocess.Popen(cmd)
        print(f"[book-complete] book_alaska.py launched for booking #{booking_id}", flush=True)
    except Exception as e:
        print(f"[book-complete] Failed to launch book_alaska.py: {e}", flush=True)


def handle_book_complete(body: dict) -> tuple[dict, int]:
    """
    POST /api/book-complete

    Vault-based booking flow:
      1. Search seats.aero for best award
      2. Pick a vault with enough miles
      3. Create a pending booking record
    """
    """
      4. Ping Appa via webhook — Appa uses the live browser session to complete booking
      5. Return booking_id so UI can poll /api/booking-status/<id>
    """
    try:
        from vault_manager import pick_vault, create_booking
    except ImportError as e:
        return {"error": f"Missing module: {e}", "step": "import"}, 500

    flight_req = body.get("flight", {})
    client_req = body.get("client", {})

    if not flight_req or not client_req:
        return {"error": "Missing required fields: flight, client"}, 400

    origin     = flight_req.get("origin", "")
    destination= flight_req.get("destination", "")
    date       = flight_req.get("date", "")
    cabin      = flight_req.get("cabin", "economy").lower()
    first_name = client_req.get("first_name", "")
    last_name  = client_req.get("last_name", "")

    if not all([origin, destination, date, first_name, last_name]):
        return {"error": "Missing required fields in flight or client"}, 400

    # ── Step 1: search for best award ────────────────────────────────────────
    step = "search_award"
    try:
        cabin_api_map = {"economy": "economy", "premium": "premium", "business": "business", "first": "first"}
        rows = search_seats_aero([origin], [destination], date, date, cabin_api_map.get(cabin, "economy"), ["aeroplan"])
        best_deal = None
        for row in rows:
            scored = score_row(row, cabin)
            if scored:
                best_deal = scored[0]
                break
        if not best_deal:
            return {"error": f"No award availability for {origin}→{destination} on {date}", "step": step}, 404
        miles_needed = best_deal["miles"]
        taxes_usd    = best_deal["taxes_usd"]
        print(f"[book-complete] Step 1 OK — {miles_needed:,} miles + ${taxes_usd} taxes")
    except Exception as e:
        return {"error": str(e), "step": step}, 500

    # ── Step 2: pick vault ───────────────────────────────────────────────────
    step = "pick_vault"
    vault = pick_vault(miles_needed)
    if not vault:
        return {
            "error": f"No vault with enough miles ({miles_needed:,} needed).",
            "step": step,
            "miles_needed": miles_needed,
        }, 503

    # ── Step 3: create pending booking ──────────────────────────────────────
    booking_id = create_booking(
        vault_id=vault["id"],
        passenger_name=f"{first_name} {last_name}",
        flight_ref=best_deal.get("availability_id", ""),
        miles_used=miles_needed,
        taxes_paid=taxes_usd,
    )
    print(f"[book-complete] Booking #{booking_id} created — notifying Appa")

    # ── Step 4: ping Appa to handle booking via browser ──────────────────────
    _ping_appa(booking_id, flight_req, client_req, best_deal)

    return {
        "status":     "pending",
        "booking_id": booking_id,
        "message":    "Booking received — being processed now. Poll /api/booking-status/" + str(booking_id),
        "flight": {
            "origin":      origin,
            "destination": destination,
            "date":        date,
            "cabin":       cabin,
            "miles":       miles_needed,
            "taxes_usd":   taxes_usd,
        },
    }, 202


def handle_booking_status(booking_id: int) -> tuple[dict, int]:
    """GET /api/booking-status/<id>"""
    try:
        from vault_manager import list_bookings
        import sqlite3
        from pathlib import Path
        conn = sqlite3.connect(str(Path(__file__).parent / "vault.db"))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
        if not row:
            return {"error": "Booking not found"}, 404
        d = dict(row)
        if d.get("status") == "confirmed" and d.get("airline_ref"):
            d["confirmation_number"] = d["airline_ref"]
        return d, 200
    except Exception as e:
        return {"error": str(e)}, 500


async def _book_award_flight(page, deal: dict, client: dict, card: dict) -> str:
    """
    Navigate to the Air Canada award booking flow, select the flight,
    fill passenger info, and pay taxes.

    Returns the booking confirmation/reference number string.
    """
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError

    origin      = deal["origin"]
    destination = deal["destination"]
    date        = deal["date"]
    cabin       = deal["cabin"]

    # Build the Air Canada award search URL
    # Format: /aeroplan/redeem/travel/flight-select?...
    search_url = (
        "https://www.aircanada.com/aeroplan/redeem/travel/flight-select"
        f"?origin={origin}&destination={destination}"
        f"&departureDate={date}&cabin={cabin}&adults=1"
    )

    print(f"[book_flight] Navigating to: {search_url}")
    await page.goto(search_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)

    # Select the first available flight
    for sel in [
        'button:has-text("Select")',
        '[data-testid*="select-flight"]',
        'button[class*="select"]',
        '[aria-label*="Select flight"]',
    ]:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                break
        except Exception:
            continue

    await page.wait_for_timeout(3000)

    # Continue through any review screens
    for label in ["Continue", "Proceed", "Review", "Next"]:
        try:
            btn = await page.query_selector(f'button:has-text("{label}")')
            if btn:
                await btn.click()
                await page.wait_for_timeout(2000)
        except Exception:
            continue

    # Fill passenger information
    for sel in ['input[name*="firstName"]', 'input[id*="firstName"]']:
        try:
            await page.fill(sel, client["first_name"])
            break
        except Exception:
            continue
    for sel in ['input[name*="lastName"]', 'input[id*="lastName"]']:
        try:
            await page.fill(sel, client["last_name"])
            break
        except Exception:
            continue

    # DOB
    dob = client.get("dob", "")
    if dob:
        for sel in ['input[name*="dob"]', 'input[name*="dateOfBirth"]']:
            try:
                await page.fill(sel, dob)
                break
            except Exception:
                continue

    # Continue to payment
    for label in ["Continue", "Proceed", "Next", "Payment"]:
        try:
            btn = await page.query_selector(f'button:has-text("{label}")')
            if btn:
                await btn.click()
                await page.wait_for_timeout(2000)
                break
        except Exception:
            continue

    # Fill card details for tax payment
    for frame in [page] + list(page.frames):
        for sel in ['input[name*="cardNumber"]', 'input[autocomplete="cc-number"]']:
            try:
                await frame.fill(sel, card["number"])
                break
            except Exception:
                continue

    # Confirm / finalize booking
    for label in ["Confirm", "Purchase", "Book", "Pay Now", "Complete"]:
        try:
            btn = await page.query_selector(f'button:has-text("{label}")')
            if btn:
                await btn.click()
                await page.wait_for_timeout(5000)
                break
        except Exception:
            continue

    # Extract confirmation number
    page_text = await page.inner_text("body")
    # Air Canada booking references are 6-character alphanumeric
    refs = re.findall(r"\b([A-Z0-9]{6})\b", page_text)
    # Filter out common non-reference uppercase sequences
    for ref in refs:
        if not all(c.isdigit() for c in ref):  # must have at least one letter
            return ref

    # Fallback: return whatever we can find
    return refs[0] if refs else "UNKNOWN"


# ── Stripe handlers ───────────────────────────────────────────────────────────

def handle_create_checkout(body: dict) -> tuple[dict, int]:
    """
    POST /api/create-checkout
    Creates a Stripe Checkout session for customer payment.
    Frontend redirects customer to the returned checkout_url.

    Required body:
        flight: { origin, destination, date, cabin }
        client: { first_name, last_name, dob, passport_number, passport_country, passport_expiry }
        miles: int (from seats.aero search)
        taxes_usd: float
        availability_id: str
    """
    try:
        from stripe_checkout import create_checkout_session
    except ImportError as e:
        return {"error": f"stripe_checkout not available: {e}"}, 500

    flight          = body.get("flight", {})
    client          = body.get("client", {})
    miles           = body.get("miles")
    taxes_usd       = body.get("taxes_usd")
    availability_id = body.get("availability_id", "")

    if not all([flight, client, miles, taxes_usd]):
        return {"error": "Missing required fields: flight, client, miles, taxes_usd"}, 400

    try:
        result = create_checkout_session(
            flight=flight,
            client=client,
            miles=int(miles),
            taxes_usd=float(taxes_usd),
            availability_id=availability_id,
        )
        return result, 200
    except Exception as e:
        return {"error": str(e), "step": "create_checkout"}, 500


def handle_stripe_webhook(raw_body: bytes, sig_header: str) -> tuple[dict, int]:
    """
    POST /api/stripe-webhook
    Receives Stripe payment confirmation and triggers full booking flow.
    """
    try:
        from stripe_checkout import verify_webhook
    except ImportError as e:
        return {"error": f"stripe_checkout not available: {e}"}, 500

    # Verify webhook signature
    try:
        event = verify_webhook(raw_body, sig_header)
    except Exception as e:
        print(f"[stripe-webhook] Signature verification failed: {e}")
        return {"error": "Invalid signature"}, 400

    if event["type"] != "checkout.session.completed":
        # Acknowledge other events but don't act on them
        return {"received": True}, 200

    session    = event["data"]["object"]
    metadata   = session.get("metadata", {})
    payment_status = session.get("payment_status")

    if payment_status != "paid":
        return {"received": True, "status": "not_paid"}, 200

    # Reconstruct booking payload from Stripe metadata
    import json as _json
    try:
        client = _json.loads(metadata.get("client_json", "{}"))
        flight = {
            "origin":      metadata.get("origin"),
            "destination": metadata.get("destination"),
            "date":        metadata.get("date"),
            "cabin":       metadata.get("cabin", "economy"),
        }
        miles            = int(metadata.get("miles", 0))
        taxes_usd        = float(metadata.get("taxes_usd", 0))
        availability_id  = metadata.get("availability_id", "")
    except Exception as e:
        return {"error": f"Failed to parse metadata: {e}"}, 400

    print(f"[stripe-webhook] Payment confirmed for {flight['origin']}→{flight['destination']} on {flight['date']}")

    # Trigger booking — use virtual card via Stripe Issuing
    try:
        from stripe_issuing import create_virtual_card, cancel_virtual_card, format_card_for_aeroplan
        # Limit = taxes + miles cost + 15% buffer
        cpp = float(os.getenv("MILES_CPP_USD", "0.0144"))
        limit_cents = int((taxes_usd + miles * cpp) * 100 * 1.15)
        label = f"{flight['origin']}-{flight['destination']}-{flight['date']}"
        virtual_card = create_virtual_card(spending_limit_cents=limit_cents, label=label)
        payment = format_card_for_aeroplan(virtual_card)
        card_id = virtual_card["card_id"]
    except Exception as e:
        print(f"[stripe-webhook] Virtual card creation failed: {e} — falling back to env card")
        # Fallback: use card from env (for testing without Stripe Issuing enabled)
        payment = {
            "card_number": os.getenv("FALLBACK_CARD_NUMBER", ""),
            "expiry":      os.getenv("FALLBACK_CARD_EXPIRY", ""),
            "cvv":         os.getenv("FALLBACK_CARD_CVV", ""),
            "cardholder_name": os.getenv("FALLBACK_CARD_NAME", ""),
        }
        card_id = None

    # Run the booking flow
    book_body = {
        "flight":          flight,
        "client":          client,
        "payment":         payment,
        "_miles_override": miles,
        "_availability_id_override": availability_id,
        "_taxes_usd_override": taxes_usd,
    }
    result, status = handle_book_complete(book_body)

    # Cancel virtual card after booking (success or fail)
    if card_id:
        cancel_virtual_card(card_id)

    return result, status


# ── HTTP server ────────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{time.strftime('%H:%M:%S')}] {fmt % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_json({"status": "ok", "service": "flight-search-api", "version": "2.0"})
        elif self.path == "/api/vault/list":
            from vault_manager import list_vaults, vault_summary
            self.send_json({"vaults": list_vaults(), "summary": vault_summary()})
        elif self.path == "/api/vault/summary":
            from vault_manager import vault_summary
            self.send_json(vault_summary())
        elif self.path.startswith("/api/booking-status/"):
            try:
                booking_id = int(self.path.split("/")[-1])
                result, status = handle_booking_status(booking_id)
                self.send_json(result, status)
            except ValueError:
                self.send_json({"error": "Invalid booking id"}, 400)
        elif self.path == "/api/discover" or self.path.startswith("/api/discover?"):
            result, status = handle_discover()
            self.send_json(result, status)
        else:
            # GET /api/trips/<id>
            m = re.match(r"^/api/trips/([^/?]+)(\?.*)?$", self.path)
            if m:
                avail_id = m.group(1)
                qs = m.group(2).lstrip("?") if m.group(2) else ""
                result, status = handle_trips(avail_id, query_string=qs)
                self.send_json(result, status)
            else:
                self.send_json({"error": "Not found"}, 404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw    = self.rfile.read(length) if length else b""

        content_type = self.headers.get("Content-Type", "")

        if self.path == "/api/discover/refresh":
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            # Also accept token in POST body or query string
            token = ""
            if raw:
                try:
                    token = json.loads(raw).get("token", "")
                except Exception:
                    pass
            if not token:
                token = qs.get("token", [""])[0]
            result, status = handle_discover_refresh(token)
            self.send_json(result, status)

        elif self.path == "/api/search":
            try:
                body = json.loads(raw)
            except Exception:
                self.send_json({"error": "Invalid JSON"}, 400)
                return
            result, status = handle_search(body)
            self.send_json(result, status)

        elif self.path == "/api/inbound-email":
            # Mailgun can send form-encoded or JSON; handle both
            if "application/json" in content_type:
                try:
                    body = json.loads(raw)
                except Exception:
                    body = {}
            else:
                # application/x-www-form-urlencoded (Mailgun default)
                import urllib.parse as _up
                body = {k: v[0] if len(v) == 1 else v
                        for k, v in _up.parse_qs(raw.decode("utf-8", errors="replace")).items()}
            result, status = handle_inbound_email(body)
            self.send_json(result, status)

        elif self.path == "/api/notify-booking":
            # Step 1: create pending record. Step 2: ask Appa for approval.
            # book_alaska.py is NOT launched until /api/booking-approve is called.
            try:
                bdata = json.loads(raw) if raw else {}
            except Exception:
                bdata = {}
            text = bdata.get("text", str(bdata))
            print(f"[notify-booking] {text}", flush=True)

            booking_id = None
            if all(k in bdata for k in ("origin", "destination", "date", "first_name", "last_name", "dob")):
                import sqlite3
                from pathlib import Path
                db_path = str(Path(__file__).parent / "vault.db")
                conn = sqlite3.connect(db_path)
                conn.execute("""
                    INSERT INTO bookings (vault_id, passenger_name, flight_ref, miles_used, taxes_paid, status)
                    VALUES (2, ?, ?, 0, 0.0, 'pending_approval')
                """, (
                    f"{bdata['first_name']} {bdata['last_name']}",
                    f"{bdata['origin']}-{bdata['destination']}-{bdata['date']}",
                ))
                conn.commit()
                booking_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                # Store full bdata as JSON in the booking row for later retrieval
                conn.execute("UPDATE bookings SET airline_ref=? WHERE id=?",
                             (json.dumps(bdata), booking_id))
                conn.commit()
                conn.close()
                print(f"[notify-booking] Created booking_id={booking_id} (pending_approval)", flush=True)

                # Notify Appa — agent will send Telegram approval request to Sami
                cabin  = bdata.get('cabin', 'business').title()
                origin = bdata.get('origin', '?')
                dest   = bdata.get('destination', '?')
                date   = bdata.get('date', '?')
                pax    = f"{bdata['first_name']} {bdata['last_name']}"
                msg = (
                    f"\U0001f3ab BOOKING REQUEST #{booking_id}\n"
                    f"Route: {origin} \u2192 {dest}\n"
                    f"Date: {date} | Cabin: {cabin}\n"
                    f"Passenger: {pax}\n"
                    f"\nApprove or deny:\n"
                    f"  approve: POST https://api.airbitrage.io/api/booking-approve "
                    f"body={{\"booking_id\":{booking_id},\"action\":\"approve\",\"token\":\"{BOOKING_APPROVE_TOKEN}\"}}\n"
                    f"  deny:    same but action=deny"
                )
                _notify_appa(msg)

            self.send_json({"ok": True, "booking_id": booking_id, "status": "pending_approval"}, 200)
            return

        elif self.path == "/api/booking-approve":
            # Called by Appa (or Sami directly) to approve or deny a pending booking.
            # Required: { booking_id, action: "approve"|"deny", token }
            try:
                bdata = json.loads(raw) if raw else {}
            except Exception:
                self.send_json({"error": "Invalid JSON"}, 400)
                return

            if bdata.get("token") != BOOKING_APPROVE_TOKEN:
                self.send_json({"error": "unauthorized"}, 403)
                return

            booking_id = bdata.get("booking_id")
            action     = bdata.get("action", "").lower()

            if not booking_id or action not in ("approve", "deny"):
                self.send_json({"error": "booking_id and action (approve|deny) required"}, 400)
                return

            import sqlite3
            from pathlib import Path
            db_path = str(Path(__file__).parent / "vault.db")
            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
            if not row:
                conn.close()
                self.send_json({"error": "Booking not found"}, 404)
                return

            # Parse stored bdata from airline_ref column (we stash JSON there temporarily)
            cols = [d[0] for d in conn.execute("PRAGMA table_info(bookings)").fetchall()]
            row_dict = dict(zip(cols, row))

            if action == "deny":
                conn.execute("UPDATE bookings SET status='denied' WHERE id=?", (booking_id,))
                conn.commit()
                conn.close()
                _notify_appa(f"\u274c Booking #{booking_id} denied. No action taken.")
                self.send_json({"ok": True, "status": "denied", "booking_id": booking_id})
                return

            # Approve: update status and launch book_alaska.py
            conn.execute("UPDATE bookings SET status='approved' WHERE id=?", (booking_id,))
            conn.commit()
            conn.close()

            try:
                stored = json.loads(row_dict.get("airline_ref", "{}"))
            except Exception:
                stored = {}

            import subprocess, sys
            script = os.path.join(os.path.dirname(__file__), "book_alaska.py")
            cmd = [
                sys.executable, script,
                "--origin",     stored.get("origin", ""),
                "--dest",       stored.get("destination", ""),
                "--date",       stored.get("date", ""),
                "--first",      stored.get("first_name", ""),
                "--last",       stored.get("last_name", ""),
                "--dob",        stored.get("dob", ""),
                "--cabin",      stored.get("cabin", "business"),
                "--card",       stored.get("card_last4", "2002"),
                "--booking-id", str(booking_id),
            ]
            print(f"[booking-approve] Approved #{booking_id}, launching book_alaska: {' '.join(cmd)}", flush=True)
            subprocess.Popen(cmd)
            _notify_appa(f"\u2705 Booking #{booking_id} approved — book_alaska.py launched. Will confirm when done.")
            self.send_json({"ok": True, "status": "approved", "booking_id": booking_id})
            return

        elif self.path == "/api/kill":
            # Kill switch: stop all running book_alaska.py processes.
            # Required: { token }
            try:
                bdata = json.loads(raw) if raw else {}
            except Exception:
                self.send_json({"error": "Invalid JSON"}, 400)
                return

            if bdata.get("token") != KILL_TOKEN:
                self.send_json({"error": "unauthorized"}, 403)
                return

            import subprocess
            result = subprocess.run(["pkill", "-f", "book_alaska.py"], capture_output=True)
            killed = result.returncode == 0

            # Also mark all pending/approved bookings as cancelled
            import sqlite3
            from pathlib import Path
            db_path = str(Path(__file__).parent / "vault.db")
            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE bookings SET status='cancelled' WHERE status IN ('pending_approval','approved')")
            conn.commit()
            conn.close()

            _notify_appa("\U0001f6d1 KILL SWITCH activated. All booking processes stopped and pending bookings cancelled.")
            self.send_json({"ok": True, "killed": killed})
            return

        elif self.path == "/api/book-complete":
            try:
                body = json.loads(raw)
            except Exception:
                self.send_json({"error": "Invalid JSON"}, 400)
                return
            result, status = handle_book_complete(body)
            self.send_json(result, status)

        elif self.path == "/api/create-checkout":
            try:
                body = json.loads(raw)
            except Exception:
                self.send_json({"error": "Invalid JSON"}, 400)
                return
            result, status = handle_create_checkout(body)
            self.send_json(result, status)

        elif self.path == "/api/stripe-webhook":
            sig = self.headers.get("Stripe-Signature", "")
            result, status = handle_stripe_webhook(raw, sig)
            self.send_json(result, status)

        elif self.path == "/api/vault/add":
            try:
                body = json.loads(raw)
            except Exception:
                self.send_json({"error": "Invalid JSON"}, 400)
                return
            from vault_manager import add_vault
            vault_id = add_vault(
                email=body["email"],
                password=body["password"],
                aeroplan_number=body["aeroplan_number"],
                miles_balance=body.get("miles_balance", 0),
            )
            self.send_json({"status": "ok", "vault_id": vault_id})

        elif self.path == "/api/vault/list":
            from vault_manager import list_vaults, vault_summary
            self.send_json({"vaults": list_vaults(), "summary": vault_summary()})

        elif self.path == "/api/vault/summary":
            from vault_manager import vault_summary
            self.send_json(vault_summary())

        else:
            self.send_json({"error": "Not found"}, 404)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8787))
    host = "0.0.0.0" if os.environ.get("RAILWAY_ENVIRONMENT") else "localhost"
    print(f"✈️  Flight Search API v2.0  →  http://{host}:{port}")
    print(f"   POST /api/search              search for award flights")
    print(f"   GET  /api/trips/<id>          flight details for an availability")
    print(f"   GET  /health                  health check")
    print(f"   GET  /api/discover            get daily discover tiles (cached)")
    print(f"   POST /api/discover/refresh    rebuild discover cache (cron use)")
    print(f"   POST /api/inbound-email       Mailgun inbound webhook (2FA codes)")
    print(f"   POST /api/create-checkout     create Stripe Checkout session")
    print(f"   POST /api/stripe-webhook      Stripe payment webhook → triggers booking")
    print(f"   POST /api/book-complete       End-to-end award booking automation")
    print()
    _load_discover_cache_from_disk()
    server = HTTPServer((host, port), Handler)
    server.serve_forever()
