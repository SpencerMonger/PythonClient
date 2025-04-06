import subprocess
import sys
import argparse
import os
from datetime import datetime

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the scripts to run in order
SCRIPTS_TO_RUN = [
    "predict_historical.py",
    "calculate_pnl.py",
    "export_pnl.py"
]

def run_script(script_name, start_date=None, end_date=None, size_multiplier=1.0):
    """Runs a given script using the same Python interpreter, passing date and size arguments if provided."""
    script_path = os.path.join(SCRIPT_DIR, script_name)
    command = [sys.executable, script_path] # Use the same python interpreter

    # Add date arguments only if they are provided and the script is one of the first two
    if start_date and end_date and script_name in ["predict_historical.py", "calculate_pnl.py"]:
        print(f"  Passing date range {start_date} to {end_date} to {script_name}")
        command.extend(["--start-date", start_date, "--end-date", end_date])
    elif script_name in ["predict_historical.py", "calculate_pnl.py"]:
        print(f"  Running {script_name} without date range filter.")
    else:
        print(f"  Running {script_name} (uses internal filters).")

    # Add size multiplier argument only for export_pnl.py
    if script_name == "export_pnl.py":
        print(f"  Passing size multiplier {size_multiplier} to {script_name}")
        command.extend(["--size-multiplier", str(size_multiplier)]) # Pass as string
    elif script_name not in ["predict_historical.py", "calculate_pnl.py"]:
         # Handle any other scripts that might be added later
         print(f"  Running {script_name} without specific arguments from run_backtest.")

    print(f"Executing command: {' '.join(command)}")
    # Run the script, capturing output and checking for errors
    # Use shell=False for security and better argument handling
    # Set bufsize=1 for line buffering (more immediate output)
    # Use text=True for easier handling of stdout/stderr
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, # Redirect stderr to stdout
        text=True,
        bufsize=1, # Line-buffered
        encoding='utf-8'
    )

    # Print output line by line as it comes
    if process.stdout:
        for line in iter(process.stdout.readline, ''):
            print(f"    [{script_name}] {line.strip()}")
        process.stdout.close()

    # Wait for the process to complete and get the return code
    return_code = process.wait()

    if return_code != 0:
        print(f"\n{'='*20} ERROR {'='*20}")
        print(f"Script {script_name} failed with exit code {return_code}.")
        print(f"Command run: {' '.join(command)}")
        print(f"Please check the output above for details.")
        print(f"{'='*47}")
        return False # Indicate failure

    return True # Indicate success

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full backtesting pipeline: predict, calculate PnL, export.")
    parser.add_argument("--start-date", type=str, help="Start date for prediction and P&L calculation (YYYY-MM-DD). Optional.")
    parser.add_argument("--end-date", type=str, help="End date for prediction and P&L calculation (YYYY-MM-DD). Optional.")
    parser.add_argument(
        "--size-multiplier",
        type=float,
        default=1.0,
        help="Multiplier to apply to the share_size during export. Default is 1.0."
    )

    args = parser.parse_args()

    start = args.start_date
    end = args.end_date
    multiplier = args.size_multiplier # Get multiplier

    # Validation
    if start and not end:
        parser.error("If --start-date is provided, --end-date is required.")
    if end and not start:
        parser.error("If --end-date is provided, --start-date is required.")
    if start and end:
        try:
            start_dt = datetime.strptime(start, '%Y-%m-%d')
            end_dt = datetime.strptime(end, '%Y-%m-%d')
            if start_dt > end_dt:
                 parser.error("Start date cannot be after end date.")
        except ValueError:
            parser.error("Invalid date format. Please use YYYY-MM-DD.")
    if multiplier <= 0:
        parser.error("Size multiplier must be a positive number.")

    print("=== Starting Backtesting Pipeline ===")
    if start and end:
        print(f"Using date range: {start} to {end}")
    else:
        print("Running without date range filter.")

    pipeline_successful = True
    for script in SCRIPTS_TO_RUN:
        print(f"\n--- Running {script} ---")
        success = run_script(script, start_date=start, end_date=end, size_multiplier=multiplier)
        if not success:
            pipeline_successful = False
            print(f"--- Pipeline halted due to error in {script} ---")
            break # Stop the pipeline if a script fails
        print(f"--- Finished {script} successfully --- ")

    print("\n===================================")
    if pipeline_successful:
        print("Backtesting Pipeline Completed Successfully.")
    else:
        print("Backtesting Pipeline Failed.")
    print("===================================")
