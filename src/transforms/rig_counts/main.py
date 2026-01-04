from pathlib import Path

import pyarrow as pa
import pandas as pd
from subsets_utils import get_data_dir, upload_data, publish
from .test import test

DATASET_ID = "baker_hughes_rig_count_weekly"

METADATA = {
    "id": DATASET_ID,
    "title": "Baker Hughes Rig Count (Weekly)",
    "description": "Weekly oil and gas rotary rig counts from Baker Hughes. The rig count is an important business barometer for the drilling industry and its suppliers. When drilling rigs are active, they consume products and services produced by the oil service industry. Active rigs indicate exploration and development activity.",
    "column_descriptions": {
        "date": "Week ending date (YYYY-MM-DD, typically Friday)",
        "region": "Geographic region (e.g., US, Canada, or US state name)",
        "rig_type": "Type of rig (Oil, Gas, Misc, or Total)",
        "count": "Number of active rigs",
    }
}


def parse_na_current(excel_path: Path) -> list[dict]:
    """Parse North America current rig count report."""
    # Try different engines
    try:
        xl = pd.ExcelFile(excel_path, engine="openpyxl")
    except Exception:
        try:
            xl = pd.ExcelFile(excel_path, engine="xlrd")
        except Exception:
            # Try reading as binary xlsb
            return []

    records = []

    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name, header=None)

        # Skip very small sheets
        if len(df) < 5 or len(df.columns) < 3:
            continue

        # Try to find header row with date-like columns
        header_row = None
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            # Look for rows that have date-like values
            date_count = 0
            for val in row:
                if pd.notna(val):
                    try:
                        pd.to_datetime(val)
                        date_count += 1
                    except Exception:
                        pass
            if date_count >= 5:  # Multiple dates = probably header
                header_row = i
                break

        if header_row is None:
            continue

        # Extract data
        dates = []
        for val in df.iloc[header_row]:
            if pd.notna(val):
                try:
                    dates.append(pd.to_datetime(val))
                except Exception:
                    dates.append(None)
            else:
                dates.append(None)

        # Process data rows
        for i in range(header_row + 1, len(df)):
            row = df.iloc[i]
            region = str(row.iloc[0]) if pd.notna(row.iloc[0]) else None

            if region is None or region.strip() == "" or region == "nan":
                continue

            # Clean region name
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
                    "rig_type": "Total",  # Sheet-level total
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

        # Find the header row
        header_row = 0
        for i in range(min(5, len(df))):
            if "State" in str(df.iloc[i, 0]) or "Location" in str(df.iloc[i, 0]):
                header_row = i
                break

        # Set column names from header row
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:]

        # Find state column
        state_col = None
        for col in df.columns:
            if "state" in str(col).lower() or "location" in str(col).lower():
                state_col = col
                break

        if state_col is None:
            state_col = df.columns[0]

        # Date columns are the rest
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
    """Transform Baker Hughes rig count data to dataset."""
    data_dir = Path(get_data_dir())

    all_records = []

    # Try parsing each available file
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

    # Deduplicate (same date, region, rig_type)
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
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
