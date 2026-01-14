"""Big Mac Index data - ingest and transform.

Data source: https://github.com/TheEconomist/big-mac-data
"""

import csv
from io import StringIO
import pyarrow as pa
from subsets_utils import get, save_raw_file, load_raw_file, upload_data, validate
from subsets_utils.testing import assert_valid_date, assert_positive

BIG_MAC_URL = "https://raw.githubusercontent.com/TheEconomist/big-mac-data/master/output-data/big-mac-full-index.csv"
DATASET_ID = "big_mac_index"


def parse_float(value: str) -> float | None:
    """Parse float value, returning None for empty/invalid."""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def test(table: pa.Table) -> None:
    """Validate Big Mac Index output."""
    validate(table, {
        "columns": {
            "date": "string",
            "country": "string",
            "iso_a3": "string",
            "currency_code": "string",
            "dollar_price": "double",
        },
        "not_null": ["date", "country", "iso_a3", "dollar_price"],
        "min_rows": 1000,
    })

    assert_valid_date(table, "date")
    assert_positive(table, "dollar_price")

    countries = set(table.column("country").to_pylist())
    assert len(countries) >= 20, f"Expected at least 20 countries, got {len(countries)}"

    dates = table.column("date").to_pylist()
    min_date = min(dates)
    max_date = max(dates)

    assert min_date < "2005-01-01", f"Expected historical data, got earliest: {min_date}"
    assert max_date > "2020-01-01", f"Expected recent data, got latest: {max_date}"

    prices = [p for p in table.column("dollar_price").to_pylist() if p is not None]
    assert max(prices) < 20, f"Suspicious high price: ${max(prices)}"
    assert min(prices) > 0.5, f"Suspicious low price: ${min(prices)}"

    print(f"  Validated {len(table):,} Big Mac records across {len(countries)} countries")


def run():
    """Ingest and transform Big Mac Index data."""
    # Ingest
    print("Fetching Big Mac Index...")
    response = get(BIG_MAC_URL, timeout=60)
    response.raise_for_status()
    save_raw_file(response.text, "big_mac_index", extension="csv")
    print(f"  Saved big_mac_index.csv ({len(response.text):,} bytes)")

    # Transform
    print("Transforming Big Mac Index data...")
    csv_text = load_raw_file("big_mac_index", extension="csv")

    records = []
    reader = csv.DictReader(StringIO(csv_text))

    for row in reader:
        date = row.get("date", "").strip()
        if not date:
            continue

        dollar_price = parse_float(row.get("dollar_price"))
        if dollar_price is None:
            continue

        record = {
            "date": date,
            "country": row.get("name", "").strip(),
            "iso_a3": row.get("iso_a3", "").strip(),
            "currency_code": row.get("currency_code", "").strip(),
            "local_price": parse_float(row.get("local_price")),
            "dollar_ex": parse_float(row.get("dollar_ex")),
            "dollar_price": dollar_price,
            "usd_raw": parse_float(row.get("USD_raw")),
            "eur_raw": parse_float(row.get("EUR_raw")),
            "gbp_raw": parse_float(row.get("GBP_raw")),
            "jpy_raw": parse_float(row.get("JPY_raw")),
            "cny_raw": parse_float(row.get("CNY_raw")),
            "gdp_per_capita": parse_float(row.get("GDP_bigmac")),
            "usd_adjusted": parse_float(row.get("USD_adjusted")),
        }
        records.append(record)

    if not records:
        raise ValueError("No Big Mac Index data found")

    records.sort(key=lambda r: (r["date"], r["country"]))

    table = pa.Table.from_pylist(records)

    countries = set(r["country"] for r in records)
    dates = [r["date"] for r in records]

    print(f"  Transformed {len(records):,} records")
    print(f"  {len(countries)} countries, {min(dates)} to {max(dates)}")

    test(table)
    upload_data(table, DATASET_ID, mode="overwrite")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
