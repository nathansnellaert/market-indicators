import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


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

    # Check we have multiple countries
    countries = set(table.column("country").to_pylist())
    assert len(countries) >= 20, f"Expected at least 20 countries, got {len(countries)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_date = min(dates)
    max_date = max(dates)

    assert min_date < "2005-01-01", f"Expected historical data, got earliest: {min_date}"
    assert max_date > "2020-01-01", f"Expected recent data, got latest: {max_date}"

    # Sanity check: Big Mac shouldn't cost more than $20 anywhere
    prices = [p for p in table.column("dollar_price").to_pylist() if p is not None]
    assert max(prices) < 20, f"Suspicious high price: ${max(prices)}"
    assert min(prices) > 0.5, f"Suspicious low price: ${min(prices)}"

    print(f"  Validated {len(table):,} Big Mac records across {len(countries)} countries")
