"""Ingest CBOE index historical data."""

from subsets_utils import get, save_raw_file

BASE_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices"

# All CBOE indices with available historical CSV data
INDICES = [
    # Volatility indices
    "VIX",       # Cboe Volatility Index (30-day implied volatility)
    "VIX1D",     # Cboe 1-Day Volatility Index
    "VIX9D",     # Cboe S&P 500 9-Day Volatility Index
    "VIX3M",     # Cboe S&P 500 3-Month Volatility Index
    "VIX6M",     # Cboe S&P 500 6-Month Volatility Index
    "VIX1Y",     # Cboe S&P 500 1-Year Volatility Index
    "VVIX",      # Cboe VIX of VIX Index (volatility of VIX)
    "SKEW",      # Cboe SKEW Index (tail risk)

    # Commodity/ETF volatility indices
    "OVX",       # Cboe Crude Oil ETF Volatility Index
    "GVZ",       # Cboe Gold ETF Volatility Index

    # Single stock volatility indices
    "VXAPL",     # Cboe Apple VIX Index
    "VXAZN",     # Cboe Amazon VIX Index
    "VXEEM",     # Cboe Emerging Markets ETF Volatility Index

    # Strategy benchmark indices - BuyWrite
    "BXM",       # Cboe S&P 500 BuyWrite Index (at-the-money)
    "BXMD",      # Cboe S&P 500 30-Delta BuyWrite Index
    "BXMW",      # Cboe S&P 500 BuyWrite Weekly Index
    "BXY",       # Cboe S&P 500 2% OTM BuyWrite Index
    "BXR",       # Cboe Russell 2000 BuyWrite Index
    "BXN",       # Cboe Nasdaq-100 BuyWrite Index
    "BXRC",      # Cboe Russell 2000 Conditional BuyWrite Index
    "BXRD",      # Cboe Russell 2000 30-Delta BuyWrite Index

    # Strategy benchmark indices - PutWrite
    "PUT",       # Cboe S&P 500 PutWrite Index
    "PUTR",      # Cboe Russell 2000 PutWrite Index
    "WPUT",      # Cboe S&P 500 One-Week PutWrite Index
    "WPTR",      # Cboe Russell 2000 One-Week PutWrite Index
    "PPUT",      # Cboe S&P 500 5% Put Protection Index

    # Strategy benchmark indices - Collar
    "CLL",       # Cboe S&P 500 95-110 Collar Index
    "CLLZ",      # Cboe S&P 500 Zero-Cost Put Spread Collar Index
    "CLLR",      # Cboe Russell 2000 Collar Index

    # Strategy benchmark indices - Other
    "CMBO",      # Cboe S&P 500 Covered Combo Index
    "BFLY",      # Cboe S&P 500 Iron Butterfly Index
    "CNDR",      # Cboe S&P 500 Iron Condor Index
    "RXM",       # Cboe Russell 2000 30-Delta BuyWrite Index
    "LOVOL",     # Cboe Low Volatility Index

    # VIX strategy indices
    "VPD",       # Cboe VIX Premium Strategy Index
    "VPN",       # Cboe Capped VIX Premium Strategy Index
    "VSTG",      # Cboe VIX Strangle Index
    "VXTH",      # Cboe VIX Tail Hedge Index

    # S&P 500 option strategy indices
    "SPRO",      # Cboe S&P 500 Risk Reversal Index
    "SPEN",      # Cboe S&P 500 Enhanced Growth Index
]


def run():
    """Fetch all CBOE index historical CSV data."""
    print(f"  Fetching {len(INDICES)} CBOE indices...")

    for i, index in enumerate(INDICES, 1):
        print(f"  [{i}/{len(INDICES)}] Fetching {index}...")

        url = f"{BASE_URL}/{index}_History.csv"
        response = get(url)
        response.raise_for_status()

        save_raw_file(response.text, index, extension="csv")
        print(f"    -> saved {index}.csv")

    print(f"  Completed fetching {len(INDICES)} indices")
