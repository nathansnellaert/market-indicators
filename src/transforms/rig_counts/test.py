import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


def test(table: pa.Table) -> None:
    """Validate rig count output."""
    # Schema validation
    validate(table, {
        "columns": {
            "date": "string",
            "region": "string",
            "rig_type": "string",
            "count": "int",
        },
        "not_null": ["date", "region", "rig_type", "count"],
        "min_rows": 1000,
    })

    # Date format validation
    assert_valid_date(table, "date")

    # Rig counts must be non-negative
    assert_positive(table, "count", allow_zero=True)

    # Check we have multiple regions
    regions = set(table.column("region").to_pylist())
    assert len(regions) >= 5, f"Expected multiple regions, got {len(regions)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_date = min(dates)
    max_date = max(dates)

    assert max_date > "2024-01-01", f"Expected recent data, got latest: {max_date}"
