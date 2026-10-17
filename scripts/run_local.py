"""Corre el pipeline completo en tu máquina con Spark + Delta OSS.

    python scripts/run_local.py --scale-factor 0.002 --start 2025-01-01 --end 2025-03-31
    python scripts/run_local.py --increment 2025-04-01   # simula la llegada de un día nuevo

Requiere: Java 17+, `pip install -e ".[dev]"`.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from novamart.config import EXPECTED_TABLES, local_config  # noqa: E402
from novamart.pipeline import run_e2e  # noqa: E402
from novamart.utils.spark import build_local_spark  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--scale-factor", type=float, default=0.002)
    p.add_argument("--start", type=date.fromisoformat, default=date(2025, 1, 1))
    p.add_argument("--end", type=date.fromisoformat, default=date(2025, 3, 31))
    p.add_argument("--increment", type=date.fromisoformat, help="genera y procesa sólo este día")
    p.add_argument("--workdir", default=str(ROOT / ".local"))
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    spark = build_local_spark(warehouse_dir=f"{args.workdir}/warehouse")
    spark.sparkContext.setLogLevel("ERROR")
    cfg = local_config(f"{args.workdir}/volume", args.scale_factor, start_date=args.start, end_date=args.end)
    if args.increment:
        timings = run_e2e(spark, cfg, gen_start=args.increment, gen_end=args.increment, gen_mode="incremental")
    else:
        timings = run_e2e(spark, cfg)
    print(timings)
    for layer, names in EXPECTED_TABLES.items():
        for name in names:
            n = spark.table(cfg.table(layer, name)).count()
            print(f"{layer:7s} {name:28s} {n:>12,}")


if __name__ == "__main__":
    main()
