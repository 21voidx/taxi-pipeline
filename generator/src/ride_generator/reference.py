from __future__ import annotations

CITIES = [
    {"city_code": "JKT", "city_name": "Jakarta", "weight": 0.55},
    {"city_code": "BDG", "city_name": "Bandung", "weight": 0.25},
    {"city_code": "SBY", "city_name": "Surabaya", "weight": 0.20},
]

ZONES = {
    "JKT": [
        ("JKT_SCBD", "SCBD", "CBD", True),
        ("JKT_SUDIRMAN", "Sudirman", "CBD", True),
        ("JKT_GAMBIR", "Gambir", "STATION", True),
        ("JKT_KELAPA_GADING", "Kelapa Gading", "MALL", False),
        ("JKT_BLOK_M", "Blok M", "TRANSIT", True),
        ("JKT_KEMANG", "Kemang", "ENTERTAINMENT", False),
    ],
    "BDG": [
        ("BDG_DAGO", "Dago", "TOURISM", True),
        ("BDG_BRAGA", "Braga", "TOURISM", True),
        ("BDG_PASTEUR", "Pasteur", "TRANSIT", False),
        ("BDG_SETIABUDI", "Setiabudi", "RESIDENTIAL", False),
        ("BDG_BUAH_BATU", "Buah Batu", "RESIDENTIAL", False),
        ("BDG_GEDUNG_SATE", "Gedung Sate", "CBD", True),
    ],
    "SBY": [
        ("SBY_TUNJUNGAN", "Tunjungan", "CBD", True),
        ("SBY_WONOKROMO", "Wonokromo", "TRANSIT", False),
        ("SBY_GUBENG", "Gubeng", "STATION", True),
        ("SBY_PAKUWON", "Pakuwon", "MALL", True),
        ("SBY_RUNGKUT", "Rungkut", "INDUSTRIAL", False),
        ("SBY_KENJERAN", "Kenjeran", "TOURISM", False),
    ],
}

CITY_WEIGHTS = [city["weight"] for city in CITIES]
CITY_CODES = [city["city_code"] for city in CITIES]
