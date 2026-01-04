from subsets_utils import get, save_raw_file

# Baker Hughes static file URLs
# Source: https://rigcount.bakerhughes.com/na-rig-count and https://rigcount.bakerhughes.com/intl-rig-count
# Note: These UUIDs may change when Baker Hughes updates files

FILES = {
    # North America - Current
    "na_current": "https://rigcount.bakerhughes.com/static-files/73462640-906f-4bd5-b691-6a1ffe5c59ed",

    # North America - Historical
    "na_2013_present": "https://rigcount.bakerhughes.com/static-files/e98bcf83-c458-4a88-8f35-4ac4d77628bb",  # 2013-Aug 2025, 11.7 MB
    "na_2000_2024": "https://rigcount.bakerhughes.com/static-files/48162dfc-eb21-4612-8b01-72743e3ed420",  # Jan 2000-Mar 2024, 691 KB
    "na_pivot_2011_2024": "https://rigcount.bakerhughes.com/static-files/009c5091-17cd-4816-9bfe-1be2c986b200",  # Feb 2011-Mar 2024 pivot, 9.5 MB
    "rigs_by_state": "https://rigcount.bakerhughes.com/static-files/e6884ae5-cee8-46a4-8c95-e03091b0aad7",  # Jan 2000-Mar 2024 by state

    # North America - Legacy/Archive
    "na_through_2016": "https://rigcount.bakerhughes.com/static-files/61a54cab-f19e-42c5-9596-d1823737aef7",  # Through 2016, 3.2 MB
    "us_annual_avg_1987_2016": "https://rigcount.bakerhughes.com/static-files/f6c7c114-b6db-4412-8c3e-8fb07c2eb7c9",  # Annual avg by state
    "us_monthly_avg_1992_2016": "https://rigcount.bakerhughes.com/static-files/4ab04723-b638-4310-afd9-294cfee00e8e",  # Monthly avg by state
    "workover_1999_2007": "https://rigcount.bakerhughes.com/static-files/f7750868-d7ee-4b95-be83-5cd5f2326d99",  # Workover rigs (discontinued)

    # International/Worldwide
    "worldwide_current": "https://rigcount.bakerhughes.com/static-files/e2f9fb51-c82b-4fe0-9f59-68b8b36d6863",  # Current month, 245 KB
    "worldwide_2013_present": "https://rigcount.bakerhughes.com/static-files/ee2f783a-97f4-4ca1-be03-e685d301fc28",  # 2013-July 2025, 1.1 MB
    "worldwide_2007_2024": "https://rigcount.bakerhughes.com/static-files/b1528137-e6e5-473a-b39e-03793ea811b0",  # Jan 2007-Mar 2024, 85 KB
    "intl_march_2024": "https://rigcount.bakerhughes.com/static-files/98667c8f-2a58-4987-ab01-83758dea2608",  # Mar 2024 detailed, 4.3 MB
    "intl_overview": "https://rigcount.bakerhughes.com/static-files/c4df27d7-5e23-456b-ab51-fc86cbc72192",  # Overview doc
}


def run():
    """Fetch all Baker Hughes rig count Excel files."""
    for name, url in FILES.items():
        print(f"  Fetching {name}...")
        try:
            response = get(url, timeout=300)
            response.raise_for_status()
            save_raw_file(response.content, name, extension="xlsx")
            print(f"    Saved {name}.xlsx ({len(response.content):,} bytes)")
        except Exception as e:
            # Some files may be removed/renamed - log and continue
            print(f"    Failed to fetch {name}: {e}")
            raise
