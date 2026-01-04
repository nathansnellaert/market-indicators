import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_month


def test_consumer_sentiment(table: pa.Table) -> None:
    """Validate consumer sentiment output."""
    validate(table, {
        "columns": {
            "month": "string",
            "index": "double",
        },
        "not_null": ["month", "index"],
        "min_rows": 100,
    })
    assert_valid_month(table, "month")
    print(f"  Validated {len(table):,} consumer sentiment records")


def test_sentiment_components(table: pa.Table) -> None:
    """Validate sentiment components output."""
    validate(table, {
        "columns": {
            "month": "string",
            "index_current_conditions": "double",
            "index_expectations": "double",
        },
        "not_null": ["month"],
        "min_rows": 100,
    })
    assert_valid_month(table, "month")
    print(f"  Validated {len(table):,} sentiment component records")


def test_inflation_expectations(table: pa.Table) -> None:
    """Validate inflation expectations output."""
    validate(table, {
        "columns": {
            "month": "string",
            "inflation_1yr": "double",
            "inflation_5yr": "double",
        },
        "not_null": ["month"],
        "min_rows": 100,
    })
    assert_valid_month(table, "month")
    print(f"  Validated {len(table):,} inflation expectation records")
