#!/usr/bin/env python3
"""Filter NYC sublet listings for 1BR/studio in downtown Manhattan or
Williamsburg, mid-May through Sep/Oct 2026.

Reads newline-delimited JSON from a file argument (default: listings.jsonl
next to this script) or stdin. Each line is an object with keys:
    url, title, neighborhood, price, bedrooms, start_date, end_date, excerpt
    sqft (optional, integer)

Dates are ISO-8601 strings (or null). Prints a markdown report of strong
matches, near-misses, and a relaxed-constraints section showing what we'd
also include if we widened the neighborhood set, allowed end dates back to
mid-August, or allowed 1.5BR.

Usage:
    python filter.py                     # reads ./listings.jsonl
    python filter.py path/to/data.jsonl
    python filter.py - < data.jsonl      # read stdin
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable

# Neighborhoods that count as "downtown Manhattan".
DOWNTOWN_MANHATTAN = {
    "financial district", "fidi", "tribeca", "soho", "noho", "nolita",
    "lower east side", "les", "east village", "west village",
    "greenwich village", "chinatown", "two bridges", "little italy",
    "battery park", "civic center", "seaport", "union square",
    "alphabet city",
}
WILLIAMSBURG = {"williamsburg", "east williamsburg", "south williamsburg"}
# User-expanded Brooklyn neighborhoods.
BROOKLYN_EXTRAS = {
    "greenpoint", "fort greene", "ft greene", "ft. greene",
    "cobble hill", "brooklyn heights",
}
TARGETS = DOWNTOWN_MANHATTAN | WILLIAMSBURG | BROOKLYN_EXTRAS

# Relaxation sets: nearby neighborhoods that aren't in the strict target
# but are close enough to be worth considering.
NEARBY_MANHATTAN = {
    "chelsea", "flatiron", "gramercy", "nomad", "murray hill",
    "upper east side", "ues", "upper west side", "uws", "hell's kitchen",
    "hells kitchen", "midtown",
}
NEARBY_BROOKLYN = {
    "dumbo", "boerum hill", "carroll gardens", "clinton hill",
    "prospect heights", "park slope", "bed stuy", "bedstuy",
    "bedford stuyvesant", "bedford-stuyvesant", "crown heights",
    "bushwick", "downtown brooklyn",
}
RELAXED_TARGETS = TARGETS | NEARBY_MANHATTAN | NEARBY_BROOKLYN

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
    sqft: int | None = None

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
            sqft=d.get("sqft"),
        )


def in_target(neighborhood: str, targets: set[str]) -> bool:
    lower = neighborhood.lower()
    return any(name in lower for name in targets)


def is_studio_or_1br(bedrooms: str, allow_1_5: bool = False) -> bool:
    b = bedrooms.lower().strip()
    if "studio" in b:
        return True
    # Reject multi-bedroom and half-bedroom formats first.
    bad = ["2br", "2 br", "2bd", "3br", "3 br", "4br", "5br",
           "2 bedroom", "3 bedroom", "4 bedroom"]
    if not allow_1_5:
        bad += ["1.5br", "1.5 br"]
    for x in bad:
        if x in b:
            return False
    if b.startswith(("1br", "1 br", "1bd", "1 bd", "1 bedroom",
                     "1-bedroom", "one bedroom", "one-bedroom")):
        return True
    if allow_1_5 and ("1.5br" in b or "1.5 br" in b):
        return True
    return False


def dates_ok(start: date | None, end: date | None, min_end: date = MIN_END) -> bool:
    if start and start > LATEST_START:
        return False
    if end and end < min_end:
        return False
    return True


def is_match(l: Listing) -> bool:
    return (
        in_target(l.neighborhood, TARGETS)
        and is_studio_or_1br(l.bedrooms)
        and dates_ok(l.start_date, l.end_date)
    )


def is_near_miss(l: Listing) -> bool:
    if is_match(l):
        return False
    if not in_target(l.neighborhood, TARGETS):
        return False
    if not is_studio_or_1br(l.bedrooms):
        return False
    return bool(l.end_date and l.end_date >= NEAR_MISS_MIN_END)


def classify_relaxed(l: Listing) -> list[str]:
    """Return a list of relaxation tags a listing matches. Empty list means
    it's either a strict match, a near-miss, or wouldn't qualify even under
    relaxation."""
    if is_match(l) or is_near_miss(l):
        return []
    tags: list[str] = []
    nbhd_in_relaxed = in_target(l.neighborhood, RELAXED_TARGETS)
    nbhd_strict = in_target(l.neighborhood, TARGETS)
    sb_strict = is_studio_or_1br(l.bedrooms)
    sb_loose = is_studio_or_1br(l.bedrooms, allow_1_5=True)
    dates_strict = dates_ok(l.start_date, l.end_date)
    dates_near = dates_ok(l.start_date, l.end_date, NEAR_MISS_MIN_END)

    if nbhd_in_relaxed and sb_strict and dates_strict and not nbhd_strict:
        tags.append("nearby-nbhd")
    if nbhd_strict and sb_loose and not sb_strict and dates_strict:
        tags.append("1.5br")
    if nbhd_in_relaxed and sb_strict and dates_near and not dates_strict and not nbhd_strict:
        tags.append("nearby-nbhd+short-end")
    return tags


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
    if l.sqft:
        lines.append(f"- **Size:** {l.sqft} sq ft")
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
    relaxed: dict[str, list[Listing]] = {}
    for l in listings:
        for tag in classify_relaxed(l):
            relaxed.setdefault(tag, []).append(l)

    print("# NYC Sublet Matches")
    print()
    print("Criteria: studio or 1BR; downtown Manhattan (FiDi, Tribeca, SoHo,")
    print("NoHo, Nolita, LES, East Village, West Village, Chinatown, Alphabet")
    print("City, etc.), Williamsburg/East Williamsburg, or these Brooklyn")
    print("neighborhoods: Greenpoint, Fort Greene, Cobble Hill, Brooklyn")
    print("Heights. Start by 2026-06-01; end on or after 2026-09-01.")
    print()
    print("Source: listingsproject.com/real-estate/new-york-city, pages 1-37")
    print("(full scrape via public browsable site).")
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
        print(f"## Near-misses: end date before Sep 1 ({len(near_misses)})")
        print()
        print("Right neighborhood and bedroom count, but end date lands between")
        print("Aug 15 and Aug 31. Worth reaching out about possible extension.")
        print()
        for i, l in enumerate(near_misses, 1):
            print(f"### {i}. {l.title}")
            print(render(l))
            print()

    if relaxed:
        print("---")
        print()
        print("## Relaxed constraints")
        print()
        print("These listings fail the strict criteria but would qualify if we")
        print("widened the neighborhood set, accepted 1.5BR, or allowed earlier")
        print("end dates. Each is tagged with the relaxation it needs.")
        print()
        order = ["nearby-nbhd", "1.5br", "nearby-nbhd+short-end"]
        section_titles = {
            "nearby-nbhd": "If we add nearby neighborhoods "
                           "(Chelsea/UES/UWS/Midtown/NoMad/Gramercy, Dumbo/Boerum "
                           "Hill/Carroll Gardens/Clinton Hill/Prospect "
                           "Heights/Park Slope/Bed-Stuy/Bushwick/Crown Heights)",
            "1.5br": "If we allow 1.5BR in the strict neighborhoods",
            "nearby-nbhd+short-end": "If we add nearby neighborhoods AND accept "
                                     "end dates back to Aug 15",
        }
        for tag in order:
            items = relaxed.get(tag, [])
            if not items:
                continue
            items = sorted(items, key=lambda x: (x.start_date or date.max,
                                                  x.end_date or date.max))
            print(f"### {section_titles[tag]} — {len(items)} listing(s)")
            print()
            for l in items:
                print(f"#### {l.title}")
                print(render(l))
                print()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
