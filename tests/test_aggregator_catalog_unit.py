#!/usr/bin/env python3
"""
Unit test that the AGGREGATOR_CATALOG (frontend-facing) stays in sync with
the Aggregator Literal (Pydantic validation) and AGGREGATOR_DIRECTION
(runtime is-improvement direction).

If these drift, the UI lies about what's accepted, or the agent climbs in
the wrong direction. Catching drift here prevents production surprises.

Run: python3 tests/test_aggregator_catalog_unit.py
"""
import os
import sys
import typing

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "auto_trader"))

from server.models.research_run import AGGREGATOR_CATALOG, Aggregator  # noqa: E402
from runner import AGGREGATOR_DIRECTION  # noqa: E402

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


# Literal values: typing.get_args returns the tuple of allowed strings.
literal_values = set(typing.get_args(Aggregator))
catalog_ids   = {entry["id"] for entry in AGGREGATOR_CATALOG}
direction_keys = set(AGGREGATOR_DIRECTION.keys())

print("\nCoverage:")
check("catalog covers every Literal value",
      literal_values == catalog_ids,
      f"\n    literal-only: {literal_values - catalog_ids}\n"
      f"    catalog-only: {catalog_ids - literal_values}")

check("AGGREGATOR_DIRECTION covers every Literal value",
      literal_values == direction_keys,
      f"\n    literal-only: {literal_values - direction_keys}\n"
      f"    direction-only: {direction_keys - literal_values}")


print("\nDirection consistency (catalog.direction ↔ AGGREGATOR_DIRECTION):")
# Catalog uses 'preserve_metric'/'minimize'/'maximize';
# runtime uses 'preserve'/'minimize'/'maximize'. Map them.
DIR_MAP = {"preserve_metric": "preserve", "minimize": "minimize", "maximize": "maximize"}
for entry in AGGREGATOR_CATALOG:
    agg = entry["id"]
    catalog_dir = DIR_MAP.get(entry["direction"])
    runtime_dir = AGGREGATOR_DIRECTION[agg]
    check(f"{agg}: catalog={entry['direction']!r} ↔ runtime={runtime_dir!r}",
          catalog_dir == runtime_dir,
          f"catalog dir mapped to {catalog_dir!r} but runtime says {runtime_dir!r}")


print("\nCatalog entry shape:")
required_keys = {"id", "label", "group", "direction", "requires_eval", "recommended", "description"}
valid_groups = {"no_aggregation", "central_tendency", "tail", "dispersion", "consistency"}
valid_dirs   = {"preserve_metric", "minimize", "maximize"}

for entry in AGGREGATOR_CATALOG:
    agg = entry["id"]
    missing = required_keys - set(entry.keys())
    check(f"{agg}: all required keys present", not missing, f"missing {missing}")
    check(f"{agg}: group is valid", entry["group"] in valid_groups,
          f"got {entry['group']!r}")
    check(f"{agg}: direction is valid", entry["direction"] in valid_dirs,
          f"got {entry['direction']!r}")
    check(f"{agg}: requires_eval is bool", isinstance(entry["requires_eval"], bool))
    check(f"{agg}: recommended is bool",   isinstance(entry["recommended"], bool))
    check(f"{agg}: description non-empty",
          isinstance(entry["description"], str) and len(entry["description"]) > 10)


print("\nBusiness rules:")
overall = next(e for e in AGGREGATOR_CATALOG if e["id"] == "overall")
check("'overall' does NOT require eval", overall["requires_eval"] is False)
for entry in AGGREGATOR_CATALOG:
    if entry["id"] != "overall":
        check(f"{entry['id']} requires eval", entry["requires_eval"] is True)

recommended = [e for e in AGGREGATOR_CATALOG if e["recommended"]]
check("exactly one aggregator is marked recommended",
      len(recommended) == 1, f"got {len(recommended)}: {[r['id'] for r in recommended]}")
check("recommended one is 'median'",
      recommended[0]["id"] == "median" if recommended else False)


print(f"\n{'=' * 50}\n{PASS} passed, {FAIL} failed\n{'=' * 50}")
sys.exit(0 if FAIL == 0 else 1)
