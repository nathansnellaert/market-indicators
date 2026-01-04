import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate Shiller S&P 500 output."""
    validate(table, {
        "columns": {
            "date": "string",
            "sp500": "double",
            "cpi": "double",
            "cape": "double",
        },
        "not_null": ["date"],
        "unique": ["date"],
        "min_rows": 1000,
    })

    dates = table.column("date").to_pylist()
    assert dates[0] < dates[-1], "Data should be chronologically sorted"

    cape_values = [c for c in table.column("cape").to_pylist() if c]
    assert min(cape_values) > 0, "CAPE should be positive"
    assert max(cape_values) < 100, "CAPE should be reasonable"

    print(f"  Validated {len(table):,} Shiller records")
