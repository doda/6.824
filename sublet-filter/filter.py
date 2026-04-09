#!/usr/bin/env python3
"""Filter NYC sublet listings for 1BR/studio in downtown Manhattan or
Williamsburg, mid-May through Sep/Oct 2026.

Reads newline-delimited JSON from a file argument (default: listings.jsonl
next to this script) or stdin. Each line is an object with keys:
    url, title, neighborhood, price, bedrooms, start_date, end_date, excerpt

Dates are ISO-8601 strings (or null). Prints a markdown report of strong
matches and near-misses.

Usage:
    python filter.py                     # reads ./listings.jsonl
    python filter.py path/to/data.jsonl
    python filter.py - < data.jsonl      # read stdin
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

# Neighborhoods that count as "downtown Manhattan".
DOWNTOWN_MANHATTAN = {
    "financial district", "fidi", "tribeca", "soho", "noho", "nolita",
    "lower east side", "les", "east village", "west village",
    "greenwich village", "chinatown", "two bridges", "little italy",
    "battery park", "civic center", "seaport", "union square",
}
WILLIAMSBURG = {"williamsburg", "east williamsburg", "south williamsburg"}
TARGETS = DOWNTOWN_MANHATTAN | WILLIAMSBURG

# Criteria: available starting no later than early June, running at least
# through Sep 1. "Mid-May start" is interpreted generously as "start by Jun 1".
LATEST_START = date(2026, 6, 1)
MIN_END = date(2026, 9, 1)
# Near-miss threshold: end date at least mid-August.
NEAR_MISS_MIN_END = date(2026, 8, 15)


@dataclass
class Listing:
    url: str
    title: str
    neighborhood: str
    price: str
    bedrooms: str
    start_date: date | None
    end_date: date | None
    excerpt: str

    @classmethod
    def from_dict(cls, d: dict) -> "Listing":
        def parse(s):
            return date.fromisoformat(s) if s else None
        return cls(
            url=d["url"],
            title=d["title"],
            neighborhood=d.get("neighborhood", ""),
            price=d.get("price", ""),
            bedrooms=d.get("bedrooms", ""),
            start_date=parse(d.get("start_date")),
            end_date=parse(d.get("end_date")),
            excerpt=d.get("excerpt", ""),
        )


def in_target_neighborhood(neighborhood: str) -> bool:
    lower = neighborhood.lower()
    return any(name in lower for name in TARGETS)


def is_studio_or_1br(bedrooms: str) -> bool:
    b = bedrooms.lower().strip()
    if "studio" in b:
        return True
    # Reject multi-bedroom and half-bedroom formats first.
    for bad in ("1.5br", "1.5 br", "2br", "2 br", "2bd", "3br", "3 br",
                "4br", "5br", "2 bedroom", "3 bedroom", "4 bedroom"):
        if bad in b:
            return False
    if b.startswith(("1br", "1 br", "1bd", "1 bd", "1 bedroom",
                     "1-bedroom", "one bedroom", "one-bedroom")):
        return True
    return False


def is_match(l: Listing) -> bool:
    if not in_target_neighborhood(l.neighborhood):
        return False
    if not is_studio_or_1br(l.bedrooms):
        return False
    if l.start_date and l.start_date > LATEST_START:
        return False
    if l.end_date and l.end_date < MIN_END:
        return False
    return True


def is_near_miss(l: Listing) -> bool:
    if is_match(l):
        return False
    if not in_target_neighborhood(l.neighborhood):
        return False
    if not is_studio_or_1br(l.bedrooms):
        return False
    if l.end_date and l.end_date >= NEAR_MISS_MIN_END:
        return True
    return False


def read_records(argv: list[str]) -> list[dict]:
    if len(argv) > 1 and argv[1] != "-":
        path = Path(argv[1])
    elif len(argv) > 1 and argv[1] == "-":
        return [json.loads(l) for l in sys.stdin if l.strip()]
    else:
        path = Path(__file__).parent / "listings.jsonl"
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def render(l: Listing, base_url: str = "https://www.listingsproject.com") -> str:
    url = l.url if l.url.startswith("http") else base_url + l.url
    lines = [
        f"- **URL:** {url}",
        f"- **Neighborhood:** {l.neighborhood}",
        f"- **Bedrooms:** {l.bedrooms}",
        f"- **Price:** {l.price}",
    ]
    if l.start_date or l.end_date:
        s = l.start_date.isoformat() if l.start_date else "?"
        e = l.end_date.isoformat() if l.end_date else "?"
        lines.append(f"- **Dates:** {s} \u2192 {e}")
    if l.excerpt:
        lines.append(f"- **Excerpt:** {l.excerpt}")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    records = read_records(argv)
    listings = [Listing.from_dict(r) for r in records]

    matches = sorted(
        (l for l in listings if is_match(l)),
        key=lambda x: (x.start_date or date.max, x.end_date or date.max),
    )
    near_misses = sorted(
        (l for l in listings if is_near_miss(l)),
        key=lambda x: x.end_date or date.min,
        reverse=True,
    )

    print("# NYC Sublet Matches")
    print()
    print("Criteria: studio or 1BR; downtown Manhattan (FiDi, Tribeca, SoHo,")
    print("NoHo, Nolita, LES, East Village, West Village, Chinatown, etc.) or")
    print("Williamsburg; start by 2026-06-01; end on or after 2026-09-01.")
    print()
    print("Source: listingsproject.com/real-estate/new-york-city, pages 1-8")
    print("(scraped via public browsable site).")
    print()
    print(f"**{len(matches)} strong match(es)** out of {len(listings)} listings scanned.")
    print()
    print("---")
    print()

    for i, l in enumerate(matches, 1):
        print(f"## {i}. {l.title}")
        print(render(l))
        print()

    if near_misses:
        print("---")
        print()
        print(f"## Near-misses ({len(near_misses)})")
        print()
        print("Right neighborhood and bedroom count, but end date before Sep 1.")
        print("Worth reaching out about possible extension.")
        print()
        for i, l in enumerate(near_misses, 1):
            print(f"### {i}. {l.title}")
            print(render(l))
            print()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
