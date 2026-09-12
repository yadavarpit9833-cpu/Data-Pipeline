"""
diagnose_firms_api.py — Find out what the FIRMS API actually accepts.

Run this when the backfill returns HTTP 400, 401 or empty bodies. It checks, in
order, the things that can be wrong, and prints the FULL response body for each
— FIRMS explains most failures in plain text that a raised-for-status exception
throws away.

    python scripts\\diagnose_firms_api.py

The MAP_KEY is redacted from every line of output, so the result is safe to
paste into a chat or an issue.

Nothing here writes to the database.
"""

import os
import sys
import json

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()
MAP_KEY = os.getenv('FIRMS_MAP_KEY', '')

BASE = 'https://firms.modaps.eosdis.nasa.gov'
AREA = '68,6,97,37'          # west,south,east,north — India
TIMEOUT = 60


def redact(text):
    text = str(text)
    if MAP_KEY:
        text = text.replace(MAP_KEY, '<MAP_KEY>')
    return text


def show(label, url, note=''):
    """Fetches a URL and prints the status plus the first part of the body."""
    print(f"\n--- {label} ---")
    if note:
        print(f"    {note}")
    print(f"    GET {redact(url)}")
    try:
        r = requests.get(url, timeout=TIMEOUT)
    except Exception as e:
        print(f"    TRANSPORT ERROR: {redact(e)}")
        return None

    body = r.text or ''
    print(f"    HTTP {r.status_code}, {len(body)} bytes")

    snippet = redact(body[:600]).strip()
    if snippet:
        for line in snippet.splitlines()[:12]:
            print(f"      | {line}")
        if len(body) > 600:
            print(f"      | ... ({len(body)} bytes total)")
    else:
        print('      | (empty body)')
    return r


def main():
    print('=' * 72)
    print(' FIRMS API DIAGNOSTIC')
    print('=' * 72)

    if not MAP_KEY:
        print('\nFIRMS_MAP_KEY is not set in .env — nothing to test.')
        return 1
    if MAP_KEY == 'your_firms_map_key_here':
        print('\nFIRMS_MAP_KEY is still the placeholder in .env.')
        return 1

    print(f"\n  MAP_KEY length : {len(MAP_KEY)} characters")
    print(f"  MAP_KEY looks  : {MAP_KEY[:4]}...{MAP_KEY[-4:]}  (a valid key is 32 hex chars)")
    print(f"  hex?           : {all(c in '0123456789abcdefABCDEF' for c in MAP_KEY)}")

    # 1. Is the key itself valid, and is it rate limited right now?
    show('1. MAP_KEY status',
         f"{BASE}/mapserver/mapkey_status/?MAP_KEY={MAP_KEY}",
         'Reports whether the key is recognised and how much quota is used.')

    # 2. Which sources exist, and what date ranges do they cover?
    #    This is the authoritative answer to "is MODIS_SP a real source name".
    show('2. Data availability (ALL sensors)',
         f"{BASE}/api/data_availability/csv/{MAP_KEY}/ALL",
         'Lists every valid source with its min/max date.')

    # 3. The simplest possible area request: NRT, 1 day, no explicit date.
    #    If THIS fails, the problem is the key or the area, not the archive.
    show('3. Area, NRT, 1 day, no date',
         f"{BASE}/api/area/csv/{MAP_KEY}/MODIS_NRT/{AREA}/1",
         'The same shape the live fetcher uses. Known to work if the key is good.')

    # 4. Add an explicit date to the NRT source.
    show('4. Area, NRT, 1 day, WITH date',
         f"{BASE}/api/area/csv/{MAP_KEY}/MODIS_NRT/{AREA}/1/2026-09-01",
         'Isolates whether adding a date is what breaks it.')

    # 5. The archive source, minimal parameters.
    show('5. Area, SP, 1 day, no date',
         f"{BASE}/api/area/csv/{MAP_KEY}/MODIS_SP/{AREA}/1",
         'Isolates whether the SP source name itself is rejected.')

    # 6. The exact request the backfill makes.
    show('6. Area, SP, 10 days, WITH date  <-- what the backfill sends',
         f"{BASE}/api/area/csv/{MAP_KEY}/MODIS_SP/{AREA}/10/2024-11-01",
         'The failing combination, reproduced exactly.')

    # 7. Same but a 1-day range, to test whether DAY_RANGE=10 is the problem.
    show('7. Area, SP, 1 day, WITH date',
         f"{BASE}/api/area/csv/{MAP_KEY}/MODIS_SP/{AREA}/1/2024-11-01",
         'If this works and 6 does not, DAY_RANGE is the issue.')

    # 8. Alternative source spellings seen in NASA documentation over time.
    for source in ('MODIS_SP', 'MODIS-SP', 'MODIS_A_SP', 'VIIRS_SNPP_SP', 'MODIS_NRT'):
        show(f"8. Source name probe: {source}",
             f"{BASE}/api/area/csv/{MAP_KEY}/{source}/{AREA}/1/2024-11-01")

    print('\n' + '=' * 72)
    print(' Paste this whole output back. The MAP_KEY is redacted throughout.')
    print('=' * 72)
    return 0


if __name__ == '__main__':
    sys.exit(main())
