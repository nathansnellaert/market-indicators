"""Baker Hughes rig counts - ingest and transform.

Data source: https://rigcount.bakerhughes.com/
"""

from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import httpx
import pyarrow as pa
import pandas as pd
from subsets_utils import get, save_raw_file, get_data_dir, upload_data, validate
from subsets_utils.testing import assert_valid_date, assert_positive

DATASET_ID = "baker_hughes_rig_count_weekly"

FILES = {
    "na_current": "https://rigcount.bakerhughes.com/static-files/73462640-906f-4bd5-b691-6a1ffe5c59ed",
    "na_2013_present": "https://rigcount.bakerhughes.com/static-files/e98bcf83-c458-4a88-8f35-4ac4d77628bb",
    "na_2000_2024": "https://rigcount.bakerhughes.com/static-files/48162dfc-eb21-4612-8b01-72743e3ed420",
    "na_pivot_2011_2024": "https://rigcount.bakerhughes.com/static-files/009c5091-17cd-4816-9bfe-1be2c986b200",
    "rigs_by_state": "https://rigcount.bakerhughes.com/static-files/e6884ae5-cee8-46a4-8c95-e03091b0aad7",
    "na_through_2016": "https://rigcount.bakerhughes.com/static-files/61a54cab-f19e-42c5-9596-d1823737aef7",
    "us_annual_avg_1987_2016": "https://rigcount.bakerhughes.com/static-files/f6c7c114-b6db-4412-8c3e-8fb07c2eb7c9",
    "us_monthly_avg_1992_2016": "https://rigcount.bakerhughes.com/static-files/4ab04723-b638-4310-afd9-294cfee00e8e",
    "workover_1999_2007": "https://rigcount.bakerhughes.com/static-files/f7750868-d7ee-4b95-be83-5cd5f2326d99",
    "worldwide_current": "https://rigcount.bakerhughes.com/static-files/e2f9fb51-c82b-4fe0-9f59-68b8b36d6863",
    "worldwide_2013_present": "https://rigcount.bakerhughes.com/static-files/ee2f783a-97f4-4ca1-be03-e685d301fc28",
    "worldwide_2007_2024": "https://rigcount.bakerhughes.com/static-files/b1528137-e6e5-473a-b39e-03793ea811b0",
    "intl_march_2024": "https://rigcount.bakerhughes.com/static-files/98667c8f-2a58-4987-ab01-83758dea2608",
    "intl_overview": "https://rigcount.bakerhughes.com/static-files/c4df27d7-5e23-456b-ab51-fc86cbc72192",
}


def is_transient_error(exception):
    """Return True for errors that should be retried."""
    if isinstance(exception, (httpx.TimeoutException, httpx.ConnectError)):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code >= 500
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=10, min=10, max=120),
    retry=retry_if_exception(is_transient_error),
    reraise=True
)
def fetch_file(url: str, timeout: int = 300):
    """Fetch a file with retry logic for transient errors."""
    response = get(url, timeout=timeout)
    response.raise_for_status()
    return response


def test(table: pa.Table) -> None:
    """Validate rig count output."""
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

    assert_valid_date(table, "date")
    assert_positive(table, "count", allow_zero=True)

    regions = set(table.column("region").to_pylist())
    assert len(regions) >= 5, f"Expected multiple regions, got {len(regions)}"

    dates = table.column("date").to_pylist()
    max_date = max(dates)
    assert max_date > "2024-01-01", f"Expected recent data, got latest: {max_date}"


def parse_na_current(excel_path: Path) -> list[dict]:
    """Parse North America current rig count report."""
    try:
        xl = pd.ExcelFile(excel_path, engine="openpyxl")
    except Exception:
        try:
            xl = pd.ExcelFile(excel_path, engine="xlrd")
        except Exception:
            return []

    records = []

    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name, header=None)

        if len(df) < 5 or len(df.columns) < 3:
            continue

        header_row = None
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            date_count = 0
            for val in row:
                if pd.notna(val):
                    try:
                        pd.to_datetime(val)
                        date_count += 1
                    except Exception:
                        pass
            if date_count >= 5:
                header_row = i
                break

        if header_row is None:
            continue

        dates = []
        for val in df.iloc[header_row]:
            if pd.notna(val):
                try:
                    dates.append(pd.to_datetime(val))
                except Exception:
                    dates.append(None)
            else:
                dates.append(None)

        for i in range(header_row + 1, len(df)):
            row = df.iloc[i]
            region = str(row.iloc[0]) if pd.notna(row.iloc[0]) else None

            if region is None or region.strip() == "" or region == "nan":
                continue

            region = region.strip()
            if region.upper() in ["TOTAL", "GRAND TOTAL", "US TOTAL"]:
                region = "US Total"

            for col_idx, date_val in enumerate(dates):
                if date_val is None or col_idx >= len(row):
                    continue

                count_val = row.iloc[col_idx]
                if pd.isna(count_val):
                    continue

                try:
                    count = int(float(count_val))
                except (ValueError, TypeError):
                    continue

                records.append({
                    "date": date_val.strftime("%Y-%m-%d"),
                    "region": region,
                    "rig_type": "Total",
                    "count": count,
                })

    return records


def parse_rigs_by_state(excel_path: Path) -> list[dict]:
    """Parse rigs by state Excel file."""
    try:
        xl = pd.ExcelFile(excel_path, engine="openpyxl")
    except Exception:
        return []

    records = []

    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name, header=None)

        if len(df) < 3:
            continue

        header_row = 0
        for i in range(min(5, len(df))):
            if "State" in str(df.iloc[i, 0]) or "Location" in str(df.iloc[i, 0]):
                header_row = i
                break

        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:]

        state_col = None
        for col in df.columns:
            if "state" in str(col).lower() or "location" in str(col).lower():
                state_col = col
                break

        if state_col is None:
            state_col = df.columns[0]

        date_cols = [c for c in df.columns if c != state_col]

        for _, row in df.iterrows():
            state = row[state_col]
            if pd.isna(state) or str(state).strip() == "":
                continue

            state = str(state).strip()

            for col in date_cols:
                try:
                    date_val = pd.to_datetime(col)
                except Exception:
                    continue

                count_val = row[col]
                if pd.isna(count_val):
                    continue

                try:
                    count = int(float(count_val))
                except (ValueError, TypeError):
                    continue

                records.append({
                    "date": date_val.strftime("%Y-%m-%d"),
                    "region": state,
                    "rig_type": sheet_name if sheet_name in ["Oil", "Gas", "Misc"] else "Total",
                    "count": count,
                })

    return records


def run():
    """Ingest and transform Baker Hughes rig count data."""
    # Ingest
    print("Fetching Baker Hughes rig count files...")
    for name, url in FILES.items():
        print(f"  Fetching {name}...")
        response = fetch_file(url, timeout=300)
        save_raw_file(response.content, name, extension="xlsx")
        print(f"    Saved {name}.xlsx ({len(response.content):,} bytes)")

    # Transform
    print("Transforming rig count data...")
    data_dir = Path(get_data_dir())
    all_records = []

    na_current_path = data_dir / "raw" / "na_current.xlsx"
    if na_current_path.exists():
        print("  Parsing NA current report...")
        records = parse_na_current(na_current_path)
        print(f"    Found {len(records):,} records")
        all_records.extend(records)

    rigs_by_state_path = data_dir / "raw" / "rigs_by_state.xlsx"
    if rigs_by_state_path.exists():
        print("  Parsing rigs by state...")
        records = parse_rigs_by_state(rigs_by_state_path)
        print(f"    Found {len(records):,} records")
        all_records.extend(records)

    if not all_records:
        raise ValueError("No records parsed from any input file")

    # Deduplicate
    seen = set()
    deduped = []
    for r in all_records:
        key = (r["date"], r["region"], r["rig_type"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    table = pa.Table.from_pylist(deduped)
    print(f"  Transformed {len(table):,} records (after dedup)")

    test(table)
    upload_data(table, DATASET_ID)


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
