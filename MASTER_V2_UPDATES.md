# Master V2 Module Updates

## Overview

The `master_v2.py` module has been updated to be more robust and reliable, especially for live data mode. Previously, the module had issues with complex SQL queries that would fail in live mode, making it unable to update the master table after inserting new data.

## Key Changes

1. **Simplified Table Structure**
   - Removed materialized views that were causing SQL errors
   - Created direct table structures with proper schemas
   - Added system marker records to initialize tables properly

2. **Improved `insert_latest_data` Function**
   - Now actually updates the master and normalized tables in live mode
   - Uses simplified SQL queries that are less prone to errors
   - Added fallback mechanisms when SQL operations fail
   - Better error handling to prevent crashes

3. **Made `init_master_v2` More Robust**
   - Added checks to avoid recreating existing tables
   - Implemented fallback to simpler table schemas when errors occur
   - Better error handling and logging

4. **Enhanced Testing Tools**
   - Updated `test_master_v2.py` with options to test different functions
   - Created `test_live_integration.py` for testing the full live workflow
   - Added `--force` flag to the initialization script for better control

5. **Improved Live Data Integration**
   - Updated `live_data.py` to work better with the master_v2 module
   - Added table existence checks before operations
   - Better error handling for model predictions

## How to Use

### Initializing the Tables

To initialize the master_v2 tables:

```bash
python init_master_v2.py
```

To force recreation of tables even if they exist:

```bash
python init_master_v2.py --force
```

### Testing Specific Functions

To test inserting latest data (default):

```bash
python test_master_v2.py
```

To test reinitializing current day's data:

```bash
python test_master_v2.py --test reinit
```

To test initializing tables:

```bash
python test_master_v2.py --test init
```

To run all tests:

```bash
python test_master_v2.py --test all
```

### Testing the Full Live Workflow

To test the complete live data workflow for a specific ticker:

```bash
python test_live_integration.py --ticker AAPL
```

### Running Live Data Collection

The regular `live_data.py` script will now properly update the master tables after fetching new data. No changes are needed to how you run it.

## Troubleshooting

If you encounter issues:

1. **Table Not Found Errors**:
   - Run `python init_master_v2.py --force` to recreate the tables

2. **SQL Syntax Errors**:
   - Check the logs for specific error details
   - If related to complex queries, the system will fall back to simpler methods

3. **No Data in Master Tables**:
   - Verify that source data is being fetched correctly
   - Check if source tables (bars, quotes, trades, indicators) have data

4. **Missing Dependencies**:
   - Ensure pytz is installed (`pip install pytz`)

## Architecture

The updated module follows this workflow in live mode:

1. Fetch data for latest minute (unchanged)
2. Store data in source tables (unchanged)
3. Check if master tables exist, create if needed
4. Clear today's data from master tables
5. Insert today's data using simple SQL JOINs
6. Update normalized tables with standardized values
7. Continue with model predictions 