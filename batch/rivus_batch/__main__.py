"""Command line for the batch steps.

    python -m rivus_batch export-raw    --hour 2026-11-11T13:00:00
    python -m rivus_batch export-stream --hour 2026-11-11T13:00:00
    python -m rivus_batch dbt           --hour 2026-11-11T13:00:00
    python -m rivus_batch load          --hour 2026-11-11T13:00:00
    python -m rivus_batch all           --hour 2026-11-11T13:00:00
    python -m rivus_batch rollup        --day 2026-11-11
"""

import argparse

from . import steps
from .common import Hour

HOUR_STEPS = {
    "export-raw": steps.export_raw,
    "export-stream": steps.export_stream,
    "dbt": steps.run_dbt,
    "load": steps.load_results,
}


def main() -> None:
    parser = argparse.ArgumentParser(prog="rivus_batch")
    parser.add_argument("step", choices=[*HOUR_STEPS, "all", "rollup"])
    parser.add_argument("--hour", help="start of the hour, UTC, such as 2026-11-11T13:00:00")
    parser.add_argument("--day", help="for rollup: YYYY-MM-DD")
    args = parser.parse_args()

    if args.step == "rollup":
        if not args.day:
            parser.error("rollup needs --day")
        steps.rollup(args.day)
        return
    if not args.hour:
        parser.error(f"{args.step} needs --hour")
    hour = Hour.parse(args.hour)
    for name in (HOUR_STEPS if args.step == "all" else [args.step]):
        HOUR_STEPS[name](hour)


if __name__ == "__main__":
    main()
