import argparse
import csv
import sys
from datetime import date
from pathlib import Path

from esma_fitrs import debt, equity


def main():
    parser = argparse.ArgumentParser(description="Explicit FITRS capture and offline CSV lookup")
    parser.add_argument("command", choices=["sync", "instrument"])
    parser.add_argument("--scope", choices=["equity", "debt"], default="equity")
    parser.add_argument("--data", type=Path, default=Path("data"))
    parser.add_argument("--isin")
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    module = equity if args.scope == "equity" else debt
    if args.command == "sync":
        result = module.sync(args.data, args.as_of)
        print(f"Captured {result['records']} records")
    else:
        if not args.isin:
            parser.error("--isin is required for instrument")
        if args.scope == "equity":
            result = equity.Transparency(args.data).instrument(args.isin, args.as_of)
        else:
            result = debt.instrument(args.data, args.isin, args.as_of)
        rows = result["records"]
        print(result["state"], file=sys.stderr)
        if rows:
            writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
