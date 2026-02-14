from pathlib import Path
import sys

from dataprocessing.datalayer import load_and_profile
from agents.query_resolution_agent import (
    run_query_resolution_agent,
    print_resolution
)


def main() -> None:
    # ------------------------------------------------------------
    # CONFIG (hardcoded for now – keep it simple)
    # ------------------------------------------------------------
    csv_path = Path("data/Amazon Sale Report.csv")
    user_query = "Give me a summary of the data."

    print(f"\nLoading file: {csv_path}")

    # ------------------------------------------------------------
    # Step 1: Data Layer
    # ------------------------------------------------------------
    try:
        table_profile = load_and_profile(csv_path)
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        sys.exit(1)

    print(f"Rows: {table_profile.total_rows:,}")
    print(f"Columns: {table_profile.total_columns}")

    # ------------------------------------------------------------
    # Step 2: Query Resolution Agent
    # ------------------------------------------------------------
    print("\nRunning Query Resolution Agent...")
    print(f"Question: {user_query}\n")

    try:
        resolution = run_query_resolution_agent(
            user_query=user_query,
            table_profile=table_profile,
        )
    except RuntimeError as exc:
        print(f"Agent error: {exc}")
        sys.exit(1)

    print_resolution(resolution)


if __name__ == "__main__":
    main()
