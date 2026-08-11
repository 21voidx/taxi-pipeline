from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP


@dataclass(frozen=True)
class Fare:
    base_fare: Decimal
    distance_fare: Decimal
    time_fare: Decimal
    surge_multiplier: Decimal
    gross_fare: Decimal
    discount_amount: Decimal
    final_fare: Decimal


def is_peak_hour(moment: datetime) -> bool:
    return moment.weekday() < 5 and (6 <= moment.hour <= 9 or 16 <= moment.hour <= 20)


def demand_multiplier(moment: datetime, city_code: str, zone_type: str) -> float:
    multiplier = 1.0
    weekend = moment.weekday() >= 5

    if is_peak_hour(moment):
        multiplier *= 1.55 if city_code == "JKT" else 1.30
    elif 11 <= moment.hour <= 13:
        multiplier *= 1.15
    elif 0 <= moment.hour <= 5:
        multiplier *= 0.35

    if weekend and zone_type in {"TOURISM", "MALL", "ENTERTAINMENT"}:
        multiplier *= 1.45
    if moment.day in {25, 26, 27, 28}:
        multiplier *= 1.20
    if city_code == "BDG" and weekend:
        multiplier *= 1.15
    return multiplier


def choose_service_type(rng: random.Random, moment: datetime, zone_type: str) -> str:
    car_probability = 0.35
    if moment.weekday() >= 5:
        car_probability += 0.10
    if zone_type in {"AIRPORT", "TOURISM", "MALL"}:
        car_probability += 0.08
    return "CAR" if rng.random() < min(car_probability, 0.65) else "BIKE"


def acceptance_probability(moment: datetime, city_code: str, is_hotspot: bool) -> float:
    probability = 0.92
    if is_peak_hour(moment):
        probability -= 0.12 if city_code == "JKT" else 0.07
    if is_hotspot:
        probability -= 0.03
    return max(0.65, probability)


def cancellation_probability(moment: datetime, arrival_delay_min: float, surge: float) -> float:
    probability = 0.055
    if is_peak_hour(moment):
        probability += 0.045
    if arrival_delay_min > 12:
        probability += 0.08
    if surge >= 1.5:
        probability += 0.035
    return min(probability, 0.32)


def surge_multiplier(moment: datetime, city_code: str, hotspot: bool, rng: random.Random) -> float:
    score = 1.0
    if is_peak_hour(moment):
        score += 0.20 if city_code == "JKT" else 0.10
    if hotspot:
        score += 0.10
    if moment.weekday() >= 5 and 18 <= moment.hour <= 23:
        score += 0.10
    score += rng.choice([0.0, 0.0, 0.0, 0.1, 0.2])
    return round(min(score, 1.8), 1)


def calculate_fare(
    service_type: str,
    distance_km: float,
    duration_min: float,
    surge: float,
    rng: random.Random,
) -> Fare:
    if service_type == "BIKE":
        base = Decimal("5000")
        distance_rate = Decimal("2300")
        time_rate = Decimal("250")
    else:
        base = Decimal("9000")
        distance_rate = Decimal("4300")
        time_rate = Decimal("500")

    distance = Decimal(str(distance_km))
    duration = Decimal(str(duration_min))
    distance_fare = distance * distance_rate
    time_fare = duration * time_rate
    subtotal = base + distance_fare + time_fare
    gross = subtotal * Decimal(str(surge))
    discount = Decimal("0")
    if rng.random() < 0.16:
        discount = min(gross * Decimal("0.10"), Decimal("15000"))
    final = max(gross - discount, Decimal("0"))

    quant = Decimal("0.01")
    return Fare(
        base.quantize(quant),
        distance_fare.quantize(quant, rounding=ROUND_HALF_UP),
        time_fare.quantize(quant, rounding=ROUND_HALF_UP),
        Decimal(str(surge)).quantize(quant),
        gross.quantize(quant, rounding=ROUND_HALF_UP),
        discount.quantize(quant, rounding=ROUND_HALF_UP),
        final.quantize(quant, rounding=ROUND_HALF_UP),
    )


def duration_from_distance(distance_km: float, service_type: str, rng: random.Random) -> float:
    average_speed = 24.0 if service_type == "BIKE" else 20.0
    traffic_factor = rng.uniform(0.85, 1.35)
    return max(5.0, math.ceil((distance_km / average_speed) * 60 * traffic_factor))
