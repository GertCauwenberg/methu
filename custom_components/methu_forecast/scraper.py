"""Scraper for met.hu settlement weather forecast.

Endpoints
---------
Autocomplete:
    GET /jquery/search.php?term=<name>
    Returns JSON list of {value, label, lat, lon, kod} objects.

Forecast:
    POST /idojaras/elorejelzes/magyarorszagi_telepulesek/main.php
    Payload (form-encoded):
        srctext=&valtozatlan=true&kod=<kod>&lt=<lat>&n=<lon>&tel=<name>&kepid=&c=tablazat
    Returns a full HTML page whose <tbody> rows are the forecast data.

Table structure (one <tr> per time slot)
-----------------------------------------
Column  CSS class(es)   Content
------  -------------   -------
0       th.naptar       Date cell (rowspan=N); 3 <div>s: month, day, weekday
1       td.ora          Time string "HH:MM"
2       td.T.X / td.T.N Min temp (class N) or Max temp (class X); blank (&nbsp;) otherwise
3       td.T            Actual temperature [°C]
4       td.idoikon      Spacer image – skip
5       td.R            Precipitation [mm]
6       td.idoikon      Weather icon <img>; tooltip text = condition description
7       td.C            Cloud cover [%]
8       td.Wikon        Wind direction icon; tooltip contains exact degrees
9       td.Wd           Wind direction text (Hungarian)
10      td.Wf           Average wind speed [km/h]
11      td.Wf           Wind gust speed [km/h]
12      td.P            Sea-level pressure [hPa]
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import aiohttp
from bs4 import BeautifulSoup, Tag

_LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.met.hu"
AC_URL   = f"{BASE_URL}/jquery/search.php"
MAIN_URL = f"{BASE_URL}/idojaras/elorejelzes/magyarorszagi_telepulesek/main.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "hu-HU,hu;q=0.9,en;q=0.8",
    "Referer": f"{BASE_URL}/idojaras/elorejelzes/magyarorszagi_telepulesek/",
    "Origin": BASE_URL,
}

# Hungarian month names → month number
HU_MONTHS = {
    "január": 1, "február": 2, "március": 3, "április": 4,
    "május": 5, "június": 6, "július": 7, "augusztus": 8,
    "szeptember": 9, "október": 10, "november": 11, "december": 12,
}

# Hungarian wind direction text → standard abbreviation
HU_WIND_DIR = {
    "északi":        "N",
    "észak-északkkeleti": "NNE",
    "északkeleti":   "NE",
    "kelet-északkeleti": "ENE",
    "keleti":        "E",
    "kelet-délkeleti": "ESE",
    "délkeleti":     "SE",
    "dél-délkeleti": "SSE",
    "déli":          "S",
    "dél-délnyugati":"SSW",
    "délnyugati":    "SW",
    "nyugat-délnyugati": "WSW",
    "nyugati":       "W",
    "nyugat-északnyugati": "WNW",
    "északnyugati":  "NW",
    "észak-északnyugati": "NNW",
    "szélcsend":     "calm",
    "változó":       "variable",
}

# met.hu icon code (from filename like w001.png, w002e.png) → HA condition
ICON_CONDITION = {
    "w001":  "sunny",           # derült (nap)
    "w001e": "clear-night",     # derült (éjjel)
    "w002":  "partlycloudy",    # kissé felhős
    "w002e": "partlycloudy",
    "w003":  "partlycloudy",    # közepesen felhős
    "w003e": "partlycloudy",
    "w004":  "cloudy",          # erősen felhős
    "w004e": "cloudy",
    "w005":  "cloudy",          # borult
    "w005e": "cloudy",
    "w006":  "partlycloudy",    # fátyolfelhős
    "w006e": "partlycloudy",
    "w007":  "fog",             # ködös
    "w007e": "fog",
    "w008":  "rainy",           # szitálás
    "w008e": "rainy",
    "w009":  "fog",             # derült, párás
    "w009e": "fog",
    "w010":  "rainy",           # gyenge eső
    "w010e": "rainy",
    "w011":  "rainy",           # eső
    "w011e": "rainy",
    "w012":  "cloudy",         # erősen fátyolfelhős
    "w012e": "cloudy",
    "w013":  "lightning-rainy", # zivatar
    "w013e": "lightning-rainy",
    "w014":  "snowy-rainy",     # havas eső
    "w014e": "snowy-rainy",
    "w015":  "snowy",           # hószállingózás
    "w015e": "snowy",
    "w016":  "snowy",           # havazás
    "w016e": "snowy",
    "w017":  "snowy",           # hózápor
    "w017e": "snowy",
    "w018":  "hail",            # jégeső
    "w018e": "hail",
    "w102":  "rainy",           # eső
    "w102e": "rainy",
    "w103":  "pouring",         # zápor
    "w103e": "pouring",
    "w104":  "lightning-rainy", # zivatar
    "w104e": "lightning-rainy",
    "w110":  "snowy-rainy",     # havas eső
    "w110e": "snowy-rainy",
}


# ---------------------------------------------------------------------------
# Public data classes
# ---------------------------------------------------------------------------

@dataclass
class Settlement:
    """A Hungarian settlement with its met.hu identifiers."""
    name: str
    kod:  str
    lat:  float
    lon:  float


@dataclass
class ForecastPeriod:
    """One row from the met.hu forecast table (typically 6-hourly)."""
    forecast_time:             datetime | None = None
    temperature:               float | None = None
    temperature_min:           float | None = None  # set on the daily min row
    temperature_max:           float | None = None  # set on the daily max row
    weather_condition:         str   | None = None
    weather_description:       str   | None = None  # Hungarian text
    precipitation:             float | None = None  # mm
    cloud_cover:               int   | None = None  # %
    wind_speed:                float | None = None  # km/h average
    wind_gust:                 float | None = None  # km/h max gust
    wind_bearing:              float | None = None  # degrees (0-360)
    wind_direction:            str   | None = None  # e.g. "NE"
    pressure:                  float | None = None  # hPa


@dataclass
class MetHuForecastData:
    """All scraped forecast data for a settlement."""
    settlement:      str  = ""
    settlement_found: bool = False
    current:         ForecastPeriod | None = None
    hourly:          list[ForecastPeriod] = field(default_factory=list)
    daily:           list[ForecastPeriod] = field(default_factory=list)
    last_updated:    datetime | None = None


# ---------------------------------------------------------------------------
# Settlement lookup (autocomplete)
# ---------------------------------------------------------------------------

async def lookup_settlement(
    session: aiohttp.ClientSession, name: str
) -> Settlement | None:
    """
    Resolve a settlement name to its met.hu identifiers.

    Calls GET /jquery/search.php?term=<name> which is the jQuery UI
    autocomplete source used on the page (visible in the page's JS).
    """
    try:
        async with session.get(
            AC_URL,
            params={"term": name},
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
    except aiohttp.ClientError as exc:
        _LOGGER.error("Settlement autocomplete failed for '%s': %s", name, exc)
        raise

    if not isinstance(data, list) or not data:
        return None

    # Try exact match first (case-insensitive), fall back to first result
    name_lower = name.lower().strip()
    best = None
    for entry in data:
        label = (entry.get("label") or entry.get("value") or "").lower()
        if label == name_lower:
            best = entry
            break
    if best is None:
        best = data[0]

    try:
        return Settlement(
            name=best.get("value") or best.get("label") or name,
            kod=str(best["kod"]),
            lat=float(best["lat"]),
            lon=float(best["lon"]),
        )
    except (KeyError, ValueError, TypeError) as exc:
        _LOGGER.error("Could not parse autocomplete entry %s: %s", best, exc)
        return None


# ---------------------------------------------------------------------------
# Forecast fetch
# ---------------------------------------------------------------------------

async def fetch_forecast(
    session: aiohttp.ClientSession, settlement: Settlement
) -> MetHuForecastData:
    """
    POST to main.php with c=tablazat and parse the returned HTML table.
    """
    payload = {
        "srctext":    "",
        "valtozatlan": "true",
        "kod":         settlement.kod,
        "lt":          str(settlement.lat),
        "n":           str(settlement.lon),
        "tel":         settlement.name,
        "kepid":       "",
        "c":           "tablazat",
    }

    _LOGGER.debug("Fetching forecast for %s (kod=%s)", settlement.name, settlement.kod)

    try:
        async with session.post(
            MAIN_URL,
            data=payload,
            headers={**HEADERS, "X-Requested-With": "XMLHttpRequest"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            html = await resp.text(encoding="utf-8", errors="replace")
    except aiohttp.ClientError as exc:
        _LOGGER.error("Forecast fetch failed for %s: %s", settlement.name, exc)
        raise

    _LOGGER.debug("Received %d bytes for %s", len(html), settlement.name)
    data = _parse_page(html, settlement.name)
    data.last_updated = datetime.now()
    return data


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def _parse_page(html: str, settlement_name: str) -> MetHuForecastData:
    result = MetHuForecastData(settlement=settlement_name)
    soup = BeautifulSoup(html, "html.parser")

    tbody = soup.find("tbody")
    if not tbody:
        _LOGGER.warning("No <tbody> found for '%s'", settlement_name)
        result.settlement_found = False
        return result

    periods = _parse_tbody(tbody)
    if not periods:
        _LOGGER.warning("No forecast rows parsed for '%s'", settlement_name)
        result.settlement_found = False
        return result

    result.settlement_found = True
    result.hourly  = periods
    result.daily   = _aggregate_daily(periods)
    result.current = _find_current(periods)
    return result


def _parse_tbody(tbody: Tag) -> list[ForecastPeriod]:
    """
    Walk every <tr> in <tbody>.

    Date context comes from <th class='naptar'> cells (which have rowspan).
    Each data row contributes one ForecastPeriod.
    """
    periods: list[ForecastPeriod] = []
    current_date: date | None = None
    year = datetime.now().year

    for tr in tbody.find_all("tr", recursive=False):
        # Check for a date header cell in this row
        naptar = tr.find("th", class_="naptar")
        if naptar:
            current_date = _parse_naptar(naptar, year)

        # The time cell tells us this is a data row
        ora_td = tr.find("td", class_="ora")
        if ora_td is None:
            continue  # header-only row, skip

        period = _parse_data_row(tr, current_date, year)
        if period is not None:
            periods.append(period)

    return periods


def _parse_naptar(th: Tag, year: int) -> date | None:
    """
    Parse a <th class='naptar'> cell.

    Structure:
        <th class='naptar' rowspan=N>
            <div>február</div>
            <div>25</div>
            <div>szerda</div>
        </th>
    """
    divs = [d.get_text(strip=True) for d in th.find_all("div")]
    if len(divs) < 2:
        return None

    month_hu = divs[0].lower()
    day_str  = divs[1]

    month = HU_MONTHS.get(month_hu)
    if month is None:
        return None
    try:
        day = int(day_str)
        # Handle year rollover (December→January)
        if month < datetime.now().month - 1:
            year += 1
        return date(year, month, day)
    except ValueError:
        return None


def _parse_data_row(tr: Tag, current_date: date | None, year: int) -> ForecastPeriod | None:
    """
    Parse a single data row into a ForecastPeriod.

    Column layout (td cells only, th.naptar already consumed):
      0  td.ora      — "HH:MM"
      1  td.T.X/N    — min/max marker
      2  td.T        — temperature
      3  td.idoikon  — spacer (skip)
      4  td.R        — precipitation mm
      5  td.idoikon  — weather icon
      6  td.C        — cloud cover %
      7  td.Wikon    — wind direction icon (degrees in tooltip)
      8  td.Wd       — wind direction text
      9  td.Wf       — wind speed km/h
      10 td.Wf       — wind gust km/h
      11 td.P        — pressure hPa
    """
    p = ForecastPeriod()

    # --- time ---
    ora = tr.find("td", class_="ora")
    if ora is None:
        return None
    p.forecast_time = _parse_time(ora.get_text(strip=True), current_date, year)

    tds = tr.find_all("td")
    # Build an index skipping the ora cell for the rest
    # We use CSS classes to find cells reliably instead of positional indexing
    # because the naptar th may or may not be in this row.

    # --- min/max + temperature ---
    t_cells = tr.find_all("td", class_="T")
    for td in t_cells:
        classes = td.get("class", [])
        raw = td.get_text(strip=True)
        val = _parse_float(raw)
        if "X" in classes:
            p.temperature_max = val
        elif "N" in classes:
            p.temperature_min = val
        else:
            p.temperature = val

    # --- precipitation ---
    r_td = tr.find("td", class_="R")
    if r_td:
        p.precipitation = _parse_float(r_td.get_text(strip=True))

    # --- cloud cover ---
    c_td = tr.find("td", class_="C")
    if c_td:
        val = _parse_float(c_td.get_text(strip=True))
        if val is not None:
            p.cloud_cover = int(val)

    # --- weather icon + description ---
    # The icon cell has class 'idoikon' and a non-spacer img src
    for ikon_td in tr.find_all("td", class_="idoikon"):
        img = ikon_td.find("img")
        if img and "spacer" not in (img.get("src") or ""):
            src = img.get("src", "")
            p.weather_condition  = _icon_src_to_condition(src)
            p.weather_description = _extract_tooltip_description(ikon_td)
            if p.weather_condition == 'exceptional':
                _LOGGER.warning("No icon_condition found for %s, description is %s", src, p.weather_description)
            break

    # --- wind direction (degrees from tooltip) ---
    wikon_td = tr.find("td", class_="Wikon")
    if wikon_td:
        p.wind_bearing = _extract_wind_degrees(wikon_td)

    # --- wind direction (text) ---
    wd_td = tr.find("td", class_="Wd")
    if wd_td:
        p.wind_direction = _hu_wind_to_abbrev(wd_td.get_text(strip=True))

    # --- wind speed and gust (both share class Wf) ---
    wf_tds = tr.find_all("td", class_="Wf")
    if len(wf_tds) >= 1:
        p.wind_speed = _parse_float(wf_tds[0].get_text(strip=True))
    if len(wf_tds) >= 2:
        p.wind_gust  = _parse_float(wf_tds[1].get_text(strip=True))

    # --- pressure ---
    p_td = tr.find("td", class_="P")
    if p_td:
        p.pressure = _parse_float(p_td.get_text(strip=True))

    return p


# ---------------------------------------------------------------------------
# Helper parsers
# ---------------------------------------------------------------------------

def _parse_time(text: str, d: date | None, year: int) -> datetime | None:
    """Parse "HH:MM" into a datetime combined with the current date context."""
    m = re.match(r"(\d{1,2}):(\d{2})", text.strip())
    if not m:
        return None
    hour, minute = int(m.group(1)), int(m.group(2))
    if d is None:
        d = date.today()
    try:
        return datetime(d.year, d.month, d.day, hour, minute)
    except ValueError:
        return None


def _parse_float(text: str) -> float | None:
    """Extract a float from a cell's text, tolerating &nbsp; and minus variants."""
    if not text:
        return None
    text = (
        text
        .replace("\xa0", "")   # non-breaking space
        .replace("−", "-")     # unicode minus
        .replace("–", "-")
        .strip()
    )
    # Handle "-0" which appears in the data
    text = re.sub(r"^-0$", "0", text)
    m = re.search(r"-?\d+\.?\d*", text)
    if m:
        try:
            return float(m.group())
        except ValueError:
            pass
    return None


def _icon_src_to_condition(src: str) -> str:
    """
    Map an icon src like '/images/idokepf24x/w002e.png' to an HA condition.
    Extracts the code (e.g. 'w002e') and looks it up in ICON_CONDITION.
    """
    m = re.search(r"/(w\d+e?)\.(?:png|gif|jpg)", src, re.I)
    if m:
        code = m.group(1).lower()
        if code in ICON_CONDITION:
            return ICON_CONDITION[code]
    return "exceptional"


def _extract_tooltip_description(td: Tag) -> str | None:
    """
    Extract the human-readable weather description from the onmouseover tooltip.

    The tooltip JS looks like:
        Tip('<div class=title>...<div class=ktext>kissé felhős</div>...')
    """
    mo = td.get("onmouseover", "")
    m = re.search(r"class=ktext>([^<]+)<", mo)
    if m:
        return m.group(1).strip()
    return None


def _extract_wind_degrees(td: Tag) -> float | None:
    """
    Extract exact wind bearing in degrees from the onmouseover tooltip.

    Tooltip text: "északkeleti\n(341 fok)"
    """
    mo = td.get("onmouseover", "")
    m = re.search(r"\((\d+)\s*fok\)", mo)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _hu_wind_to_abbrev(text: str) -> str | None:
    """Convert a Hungarian wind direction name to a compass abbreviation."""
    key = text.lower().strip()
    return HU_WIND_DIR.get(key, text or None)


# ---------------------------------------------------------------------------
# Daily aggregation
# ---------------------------------------------------------------------------

def _find_current(periods: list[ForecastPeriod]) -> ForecastPeriod | None:
    """Return the period closest to now (first future or last past).
       met.hu provides updates per 6 hours, so we subtract 3 hours from now"""
    now = datetime.now() - timedelta(hours=3)
    future = [p for p in periods if p.forecast_time and p.forecast_time >= now]
    if future:
        return future[0]
    return periods[-1] if periods else None


def _aggregate_daily(periods: list[ForecastPeriod]) -> list[ForecastPeriod]:
    """Aggregate per-slot periods into one summary ForecastPeriod per calendar day."""
    from collections import defaultdict

    groups: dict[str, list[ForecastPeriod]] = defaultdict(list)
    for p in periods:
        key = p.forecast_time.strftime("%Y-%m-%d") if p.forecast_time else "unknown"
        groups[key].append(p)

    daily: list[ForecastPeriod] = []
    for key, day_periods in groups.items():
        s = ForecastPeriod()

        # Anchor daily period to noon
        if day_periods[0].forecast_time:
            s.forecast_time = day_periods[0].forecast_time.replace(
                hour=12, minute=0, second=0, microsecond=0
            )

        temps = [p.temperature for p in day_periods if p.temperature is not None]
        if temps:
            s.temperature     = round(sum(temps) / len(temps), 1)
            s.temperature_min = min(temps)
            s.temperature_max = max(temps)

        # Prefer explicit min/max markers if present
        explicit_min = [p.temperature_min for p in day_periods if p.temperature_min is not None]
        explicit_max = [p.temperature_max for p in day_periods if p.temperature_max is not None]
        if explicit_min:
            s.temperature_min = min(explicit_min)
        if explicit_max:
            s.temperature_max = max(explicit_max)

        precips = [p.precipitation for p in day_periods if p.precipitation is not None]
        if precips:
            s.precipitation = round(sum(precips), 1)

        winds = [p.wind_speed for p in day_periods if p.wind_speed is not None]
        if winds:
            s.wind_speed = max(winds)

        gusts = [p.wind_gust for p in day_periods if p.wind_gust is not None]
        if gusts:
            s.wind_gust = max(gusts)

        pressures = [p.pressure for p in day_periods if p.pressure is not None]
        if pressures:
            s.pressure = round(sum(pressures) / len(pressures), 1)

        # Use the midday (or nearest) slot for condition, direction, cloud cover
        mid = day_periods[len(day_periods) // 2]
        s.weather_condition   = mid.weather_condition
        s.weather_description = mid.weather_description
        s.wind_direction      = mid.wind_direction
        s.wind_bearing        = mid.wind_bearing
        s.cloud_cover         = mid.cloud_cover

        daily.append(s)

    return daily
