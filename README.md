# Across Analytics

[recording.webm](https://github.com/user-attachments/assets/0b0d9e3b-3422-4cfc-9bfc-79a26cd65e4f)

Pipeline for extracting on-chain Across protocol data, transforming it for analysis, and loading it into a PostgreSQL database to feed Superset dashboard.

## Setup

1.  **Prerequisites**: Python 3.10+
2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Environment Variables**: Create a `.env` file in the root directory.
    ```ini
    # API Keys
    ALCHEMY_API_KEY=your_key
    ALCHEMY_API_KEY_2=your_secondary_key      
    ETHERSCAN_API_KEY=your_key
    MORALIS_API_KEY=your_key

    # Database
    POSTGRES_HOST=localhost
    POSTGRES_DB=across_analytics
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=password
    POSTGRES_PASSWORD=password
    ```

## 🏗 How It Works

Three simple steps to get data from the blockchain to your dashboard:

1.  **Extract**: Scripts talk to the blockchain (Alchemy/Etherscan) and download raw event logs (Deposits, Fills) into files.
2.  **Transform**: Python scripts decode the gibberish hex data into actual numbers and dates, saving them as efficient Parquet files.
3.  **Load**: We push the processed Parquet files into the PostgreSQL database as raw tables.
4.  **Refine (dbt)**: Once in the DB, **dbt** runs SQL magic to match deposits with fills, calculate fees & speeds, and build clean "marts" ready for your dashboard.
5.  **Visualize**: We connect **Apache Superset** to the database to build Across Analytics dashboard with charts for insights.

## Usage

### 1. Configuration
The pipeline is controlled by `src/config.py`. You **must** review this file before running any scripts. Here is what you can change:

#### `RUN_CONFIG` (The Main Control Knob)
Controls *what* data is extracted and for *when*.
```python
RUN_CONFIG = {
    # 1. Chain Selection: Comment/Uncomment chains to Include/Exclude them from the run.
    #    - chains_etherscan: Uses Etherscan API ( for Arbitrum, Polygon, Ethereum, ...)
    #    - chains_alchemy: Uses Alchemy API ( for Optimism, Base, Binance chain (BSC))
    "chains_etherscan": [
        "ethereum", 
        "arbitrum", 
        # "polygon",  <-- Commented out: will NOT be extracted
    ],
    "chains_alchemy": [
        "optimism",
        "base",
        "bsc"
    ],
    
    # 2. Date Range: YYYY-MM-DD format.
    #    - Defines the window for log extraction.
    #    - Price extraction automatically adds ±1 day buffer to this range allwing for data overlap.
    "start_date": "2026-01-05",
    "end_date": "2026-01-06",
}
```

#### `TOKENS_PRICES`
Controls which tokens we fetch historical prices for.
```python
TOKENS_PRICES = {
    "tokens_to_fetch": [
        "USDC", "WETH", "ACX", "HYPE" # Add any CoinGecko-supported symbol here
    ]
}
```

#### `ETL_CONFIG` (better to leave as is, if data not extracting due to rate lmit errors, then reduce these params)
Tuning parameters for API performance. detailed `config.py`:
- `chunk_size`: How many blocks to query at once (Warning: higher = more timeout risk).
- `rate_limit_page`: Seconds to wait between pages (increase if getting 429 errors).
- `max_retries`: How many times to retry a failed request before giving up.

### 2. Extraction
There are three extraction scripts depending on the data source:

**Log Extraction (Alchemy)**
Best for high-volume chains (Optimism, Base). Extracts logs and enriches with gas data.
```bash
python src/etl/extract/extract_logs_from_alchemy.py
```

**Log Extraction (Etherscan)**
Best for long-tail chains. Uses the standard Etherscan API.
```bash
python src/etl/extract/extract_logs_from_etherscan.py
```

**Token Price Extraction**
Fetches historical prices for tokens defined in `config.py`. Supports CLI arguments:
```bash
# Use defaults from config.py - date range for prices will be adjusted for +-1 day to account for data overlap
python src/etl/extract/extract_prices_from_alchemy.py

```

```bash
# Verify downloaded data integrity
python src/etl/extract/validate_extracted_data.py
```

### 3. Transformation
Decodes raw JSONL logs into structured Parquet files.
```bash
python src/etl/transform/transform_logs.py
```
*Note*: This processes all `.jsonl` files found in `data/raw/` and saves them to `data/processed/`.

```bash
# Check Parquet file quality
python src/etl/transform/validate_transform.py
```

### 4. Load

```bash
# Verify parquet files quality before load into db
python src/etl/load/validate_before_database_load.py
```

Loads processed Parquet files into the PostgreSQL database.
```bash
python src/etl/load/load_logs_processed_to_database.py
```
*Note*: Currently performs a full refresh (truncates tables) for the files being loaded.

```bash
# Verify row counts in DB match files
python src/etl/load/validate_database_after_load.py
```

### 5. dbt (Data Transformation)
After loading raw data, use dbt to materialize marts.
```bash
cd dbt
dbt run
```

## Structure
## 📁 Project Structure

### `src/` (Source Code)
Contains all Python ETL logic.
- **`etl/extract/`**: Scripts to fetch raw logs from Alchemy/Etherscan and prices.
    - `extract_logs_from_alchemy.py`: Main extractor. Fetches logs + Transaction Receipts (gas used) for Alchemy-supported chains.
    - `extract_logs_from_etherscan.py`: Fallback extractor for other chains (logs only, no gas data).
    - `extract_prices_from_alchemy.py`: Fetches hourly token prices for USD conversion.
    - `validate_extracted_data.py`: Checks raw downloaded files to ensure no blocks were skipped and no data is duplicated.
- **`etl/transform/`**: Polars-based logic to transform logs into structured data.
    - `transform_logs.py`: Decodes hex inputs/outputs, parses internal transactions, and saves as Parquet.
    - `validate_transform.py`: Checks the processed Parquet files for correct column types and non-empty results.
- **`etl/load/`**: Logic to load Parquet files into PostgreSQL.
    - `load_logs_processed_to_database.py`: Loads structured Parquet files into raw DB tables (Truncate + Load).
    - `validate_before_database_load.py`: Sanity checks on files before touching the DB.
    - `validate_database_after_load.py`: Verifies that the number of rows in the DB matches the files.
- **`config.py`**: Central configuration for chains, dates, and API settings.

### `data/` (Data Store)
- **`raw/`**: unprocessed extacted logs from APIs
    - `etherscan_api/`: JSONL logs from Etherscan.
    - `alchemy_api/`: JSONL logs from Alchemy.
    - `prices/`: CSV token price history.
- **`processed/`**: Decoded Parquet files ready for loading into db.
- **`seeds/`**: tokens_contracts_per_chain.json for token metadata etc.

### `dbt/` (Data Warehouse)
SQL transformations managed by dbt.
- **`models/`**:
    - **`staging/`**: Raw views. One model per chain (e.g., `stg_ethereum_logs`). Cleans column names and distincts data.
    - **`intermediate/`**: Complex logic.
        - `int_unified_deposits/fills`: Unions data from *all* chains into one big table.
        - `int_deposit_fill_matching`: The "Brain". Matches a Deposit on Chain A to its corresponding Fill on Chain B using `deposit_id`.
    - **`marts/`**: Final tables for dashboards (BI-ready).
        - `mart_general_overview`: High-level volume and count KPIs.
        - `mart_bridge_fee_analysis`: Profitability and fee breakdown.
        - `mart_fill_latency_analysis`: How fast are bridges happening?
        - `mart_route_concentration_risk`: Relayer decentralization metrics.
- **`seeds/`**: CSV files that get loaded into the database as static tables.
    - `chain_metadata.csv`: Map of Chain IDs (e.g. `1`) to names (`Ethereum`).
    - `token_metadata.csv`: Details about tokens (Symbol, Decimals, Address).
    - `token_prices.csv`: Historical prices used to calculate USD values. - generated by extract_prices_from_alchemy.py and settings in config.py
- **`dbt_project.yml`**  - all dbt settings for this project

## 🔧 Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| **429 Too Many Requests** | API rate limit hit. | Increase `rate_limit_page` in `src/config.py` or upgrade API plan. |
| **Relation "raw.x" does not exist** | Database table missing. | Run `load_logs_processed_to_database.py` to create raw tables, then `dbt run`. |
