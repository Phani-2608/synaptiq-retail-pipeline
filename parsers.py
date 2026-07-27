"""
Pure parsing logic extracted from 02_silver.py, expressed as plain Python so it
can be unit tested without a Spark session.

The notebook uses the Spark SQL equivalents of these (coalesce of try_to_date /
try_cast expressions) for performance at scale. This module exists so the parsing
*rules* — which format wins, what counts as malformed, how ambiguity resolves —
are independently testable and reviewable without spinning up a cluster.

Run: python parsers.py
"""

import re
from datetime import date, datetime, timezone


def parse_order_date(raw: str):
    """
    Three formats supported: ISO (yyyy-MM-dd), US slash (MM/dd/yyyy), and a
    10-digit Unix epoch. Returns a date, or None if unparseable (caller routes
    None to quarantine).

    The slash format is genuinely ambiguous (01/02/2024 = Jan 2 or Feb 1). This
    function resolves it as US-style MM/dd/yyyy — a documented assumption, not
    a fact; see NOTES.md §3.
    """
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None

    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        pass

    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except ValueError:
        pass

    if re.fullmatch(r"\d{10}", s):
        return datetime.fromtimestamp(int(s), tz=timezone.utc).date()

    return None


def parse_unit_price(raw: str):
    """
    Strips currency symbols and thousands separators before casting.
    Returns a float (the notebook casts to DECIMAL at the Spark boundary),
    or None if the result isn't numeric.
    """
    if raw is None:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", raw.strip())
    if cleaned in ("", "-", "."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_region(raw: str) -> str:
    """Title-cases and maps missing/empty to 'Unknown'."""
    if raw is None or not raw.strip():
        return "Unknown"
    return raw.strip().title()


# --------------------------------------------------------------------------
# Assertions. Not a framework — a first pass proving the rules hold, and the
# concrete place I'd plug pytest in with more time (see NOTES.md §8).
# --------------------------------------------------------------------------

def _run_checks():
    checks = []

    checks.append(("ISO date",
        parse_order_date("2024-01-02") == date(2024, 1, 2)))

    checks.append(("US slash date",
        parse_order_date("01/02/2024") == date(2024, 1, 2)))

    checks.append(("Unix epoch (2024-01-02 UTC)",
        parse_order_date("1704153600") == date(2024, 1, 2)))

    checks.append(("Unparseable date -> None",
        parse_order_date("not-a-date") is None))

    checks.append(("Empty date -> None",
        parse_order_date("") is None))

    checks.append(("Plain price",
        parse_unit_price("24.99") == 24.99))

    checks.append(("Currency symbol + thousands separator",
        parse_unit_price("$1,099.00") == 1099.00))

    checks.append(("Bare decimal",
        parse_unit_price("3.50") == 3.50))

    checks.append(("Malformed price -> None",
        parse_unit_price("N/A") is None))

    checks.append(("Missing price -> None",
        parse_unit_price(None) is None))

    checks.append(("Lowercase region normalizes",
        parse_region("west") == "West"))

    checks.append(("Already-correct region unchanged",
        parse_region("North") == "North"))

    checks.append(("Missing region -> Unknown",
        parse_region(None) == "Unknown"))

    checks.append(("Empty region -> Unknown",
        parse_region("   ") == "Unknown"))

    failed = [name for name, ok in checks if not ok]
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")

    print(f"\n{len(checks) - len(failed)}/{len(checks)} passed")
    if failed:
        raise SystemExit(f"FAILED: {failed}")


if __name__ == "__main__":
    _run_checks()
