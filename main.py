from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from dataprocessing.datalayer import load_and_profile
from agents.query_resolution_agent import build_metadata_context, trim_history
from graph import build_graph
from state import RetailAgenticState


def main() -> None:
    parser = argparse.ArgumentParser(description="Retail Insights Assistant")
    parser.add_argument("--csv", default="data/Amazon Sale Report.csv")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"\nFile not found: {csv_path}")
        sys.exit(1)

    # Load dataset
    print(f"\nLoading {csv_path.name} ...", flush=True)
    table_profile = load_and_profile(csv_path)
    print(f"Loaded {table_profile.total_rows:,} rows and {table_profile.total_columns} columns")

    metadata_str = build_metadata_context(table_profile)

    # Create DuckDB connection
    import duckdb
    db_con = duckdb.connect(database=":memory:")
    db_con.execute(
        f"CREATE TABLE {table_profile.table_name} AS "
        f"SELECT * FROM read_csv_auto('{csv_path.resolve()}', header=true, ignore_errors=true, sample_size=-1)"
    )

    print("Building graph...")
    graph = build_graph()
    print("Ready.\n")

    chat_history: list[dict] = []

    print("Retail Insights Assistant (type 'exit' to quit)\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("exit", "quit", "q"):
            print("\nExiting.")
            break

        chat_history.append({"role": "user", "content": user_input})

        initial_state: RetailAgenticState = {
            "table_metadata": metadata_str,
            "db_con": db_con,
            "table_name": table_profile.table_name,
            "user_query": user_input,
            "chat_history": list(chat_history),

            "resolution": None,
            "sql": "",
            "rows": [],
            "columns": [],

            "validation_passed": False,
            "validation_reason": "",
            "validation_feedback": "",
            "route_to": "",
            "resolution_retry_count": 0,
            "extraction_retry_count": 0,

            "final_answer": "",
            "messages": [],
            "error": None,
        }

        print("Thinking...", flush=True)

        try:
            final_state: RetailAgenticState = graph.invoke(initial_state)
        except Exception as exc:
            answer = f"Something went wrong: {exc}"
            print(f"\nAssistant:\n{answer}")
            chat_history.append({"role": "assistant", "content": answer, "query_spec": None})
            chat_history = trim_history(chat_history)
            continue

        sql = final_state.get("sql", "")
        print(f"SQL: {sql}")

        resolution_retry = final_state.get("resolution_retry_count")
        print(f"Resolution retries: {resolution_retry}")

        extraction_retry = final_state.get("extraction_retry_count")
        print(f"Extraction retries: {extraction_retry}")

        answer = final_state.get("final_answer") or "No answer was produced."

        resolution = final_state.get("resolution")
        query_spec_dict = resolution.model_dump() if resolution else None

        chat_history.append({
            "role": "assistant",
            "content": answer,
            "query_spec": query_spec_dict,
        })

        chat_history = trim_history(chat_history)

        print(f"\nAssistant:\n{answer}")

        if os.getenv("DEBUG") and sql:
            print("\n[SQL]")
            print(sql)

    db_con.close()


if __name__ == "__main__":
    main()
