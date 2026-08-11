from __future__ import annotations

import argparse
import logging

from .config import Settings
from .service import RideGenerator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Synthetic ride-hailing data generator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser("bootstrap", help="Create historical data")
    bootstrap.add_argument("--days", type=int)
    bootstrap.add_argument("--rides-per-day", type=int)
    bootstrap.add_argument("--customers", type=int)
    bootstrap.add_argument("--drivers", type=int)

    realtime = subparsers.add_parser("realtime", help="Continuously create and update rides")
    realtime.add_argument("--once", action="store_true", help="Run one tick only")

    seed = subparsers.add_parser("seed", help="Seed reference and master data")
    seed.add_argument("--customers", type=int)
    seed.add_argument("--drivers", type=int)

    subparsers.add_parser("crud-demo", help="Create/read/update/delete a temporary customer")
    return parser


def selected(value: int | None, default: int) -> int:
    return default if value is None else value


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    settings = Settings()
    generator = RideGenerator(settings)
    args = build_parser().parse_args()

    if args.command == "bootstrap":
        generator.bootstrap(
            days=selected(args.days, settings.bootstrap_days),
            rides_per_day=selected(
                args.rides_per_day,
                settings.bootstrap_rides_per_day,
            ),
            customer_count=selected(args.customers, settings.customer_count),
            driver_count=selected(args.drivers, settings.driver_count),
        )
    elif args.command == "realtime":
        if args.once:
            generator.seed_reference_and_master()
            generator.create_realtime_rides(settings.rides_per_tick)
            logging.info("Lifecycle updates: %s", generator.progress_realtime())
        else:
            generator.realtime_loop()
    elif args.command == "seed":
        generator.seed_reference_and_master(
            customer_count=selected(args.customers, settings.customer_count),
            driver_count=selected(args.drivers, settings.driver_count),
        )
    elif args.command == "crud-demo":
        generator.crud_demo()


if __name__ == "__main__":
    main()
