import pytz
from datetime import datetime, timedelta

def check_time_calculation():
    """Check how the live_data.py script calculates time ranges"""
    # Get current time in Eastern Time
    et_tz = pytz.timezone('US/Eastern')
    now = datetime.now(et_tz)
    print(f"Current time (ET): {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Calculate the "current minute" (which is the minute we're in)
    current_minute = now.replace(second=0, microsecond=0)
    print(f"Current minute (with seconds=0): {current_minute.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Calculate the next minute (this is when we'll run)
    next_minute = current_minute + timedelta(minutes=1)
    print(f"Next minute: {next_minute.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Target time is just 1 second into the next minute (when live_data.py will run)
    target_time = next_minute.replace(second=1)
    print(f"Target time (when we'll run): {target_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Calculate wait time
    wait_time = (target_time - now).total_seconds()
    print(f"Wait time until run: {wait_time:.2f} seconds")
    
    # After waiting, the script calculates the previous minute's time range
    # (in real operation, "now" would be updated after sleeping)
    last_minute_end = now.replace(second=0, microsecond=0)  # Current minute start
    last_minute_start = last_minute_end - timedelta(minutes=1)  # Previous minute start
    
    print("\nPrevious minute we'd process:")
    print(f"From: {last_minute_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"To:   {last_minute_end.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Check if there's an overlap with the latest data in the database
    latest_data_time = datetime(2025, 3, 27, 13, 50, 38, tzinfo=et_tz)
    print(f"\nLatest data time found: {latest_data_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Calculate the gap between the latest data and the current time
    gap_minutes = round((now - latest_data_time).total_seconds() / 60)
    print(f"Gap between latest data and now: ~{gap_minutes} minutes")
    
    # For live mode, we'd process just the previous minute
    if gap_minutes > 1:
        print("\nPotential issue: There appears to be a gap of multiple minutes between the latest")
        print(f"data in the database ({latest_data_time.strftime('%H:%M:%S')}) and the current time ({now.strftime('%H:%M:%S')})")
        print("The live_data.py script only processes the previous minute, which may explain")
        print("why no data is being collected for the current timestamp.")
    
if __name__ == "__main__":
    check_time_calculation() 