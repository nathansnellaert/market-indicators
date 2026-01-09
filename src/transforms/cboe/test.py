import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


def test(table: pa.Table) -> None:
    """Validate CBOE indices output."""
    validate(table, {
        "columns": {
            "date": "string",
            "index": "string",
            "category": "string",
            "close": "double",
        },
        "not_null": ["date", "index", "category", "close"],
        "min_rows": 10000,
    })

    assert_valid_date(table, "date")
    assert_positive(table, "close")

    # Check we have multiple indices
    indices = set(table.column("index").to_pylist())
    assert len(indices) >= 10, f"Expected at least 10 indices, got {len(indices)}"

    # Check we have recent data
    dates = table.column("date").to_pylist()
    max_date = max(dates)
    assert max_date > "2024-01-01", f"Expected recent data, got latest: {max_date}"

    # Check categories are valid
    valid_categories = {
        "volatility", "commodity_volatility", "single_stock_volatility",
        "buywrite", "putwrite", "collar", "other_strategy",
        "vix_strategy", "sp500_strategy", "other"
    }
    categories = set(table.column("category").to_pylist())
    invalid = categories - valid_categories
    assert not invalid, f"Invalid categories: {invalid}"

    print(f"  Validated {len(table):,} CBOE records across {len(indices)} indices")
