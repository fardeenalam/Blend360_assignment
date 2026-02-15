from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb


# Profile for a single column, should be later consumed by the schema agent
@dataclass
class ColumnProfile:
    name: str
    dtype: str                          # DuckDB native type string
    sample_values: list[Any]            # Up to 5 distinct values
    distinct_count: int                 # Total number of distinct (non-null) values
    null_count: int                     # Number of NULL rows
    total_rows: int                     # Total rows in the table
    high_cardinality: bool              # True if distinct_count > CARDINALITY_THRESHOLD
    min_val: float | None = None
    max_val: float | None = None
    avg_val: float | None = None


# Full profile of the table
@dataclass
class TableProfile:
    table_name: str
    file_path: str
    total_rows: int
    total_columns: int
    columns: list[ColumnProfile] = field(default_factory=list)


EXCEEDS_LIMIT: int = 50       #To be used to flag if >5 distinct values
MAX_SAMPLE_VALUES: int = 5   #Number of distinct values to fetch to be fed as examples
NUMERIC_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT",
    "HUGEINT", "FLOAT", "DOUBLE", "DECIMAL", "REAL",
    "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT",
}


def load_and_profile(csv_path: str | Path) -> TableProfile:
    """This function loads the csv into duckdb and reutrns a full table profile"""

    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")
    
    table_name = csv_path.stem.lower().replace(" ", "_").replace("-", "_")

    con = duckdb.connect(database=":memory:")

    # loading the csv file
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path.resolve()}', header=true)  
        """
    )

    # Total row count
    total_rows: int = con.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]

    # schema - column names + types
    schema_rows = con.execute(
        f"PRAGMA table_info('{table_name}')"
    ).fetchall()


    column_profiles: list[ColumnProfile] = []

    # This loop process for each column in the table, to enrich the agents
    # further in the pipeline with some sort of metadata
    for _, col_name, col_type, *_ in schema_rows:
        col_type_upper = col_type.upper().split("(")[0].strip()

        # null count
        null_count: int = con.execute(
            f'SELECT COUNT(*) FROM {table_name} WHERE "{col_name}" IS NULL'
        ).fetchone()[0]

        # distinct non-null values
        distinct_count: int = con.execute(
            f'SELECT COUNT(DISTINCT "{col_name}") FROM {table_name} WHERE "{col_name}" IS NOT NULL'
        ).fetchone()[0]

        high_cardinality = distinct_count > EXCEEDS_LIMIT

        # Sample distinct values, provide the agent with a sample of 5 rows
        # to help with some context on what the data actually looks like
        sample_rows = con.execute(
            f"""
            SELECT DISTINCT "{col_name}"
            FROM {table_name}
            WHERE "{col_name}" IS NOT NULL
            ORDER BY RANDOM()
            LIMIT {MAX_SAMPLE_VALUES}
            """
        ).fetchall()
        sample_values = [row[0] for row in sample_rows]

        min_val = max_val = avg_val = None
        if col_type_upper in NUMERIC_TYPES:
            stats = con.execute(
                f"""
                SELECT
                    MIN("{col_name}"),
                    MAX("{col_name}"),
                    AVG("{col_name}")
                FROM {table_name}
                WHERE "{col_name}" IS NOT NULL
                """
            ).fetchone()
            if stats:
                min_val, max_val, avg_val = (
                    float(stats[0]) if stats[0] is not None else None,
                    float(stats[1]) if stats[1] is not None else None,
                    float(stats[2]) if stats[2] is not None else None,
                )

        column_profiles.append(
            ColumnProfile(
                name=col_name,
                dtype=col_type,
                sample_values=sample_values,
                distinct_count=distinct_count,
                null_count=null_count,
                total_rows=total_rows,
                high_cardinality=high_cardinality,
                min_val=min_val,
                max_val=max_val,
                avg_val=avg_val,
            )
        )

#  if group by in high cardinality necessary always apply limit
    con.close()

    return TableProfile(
        table_name=table_name,
        file_path=str(csv_path.resolve()),
        total_rows=total_rows,
        total_columns=len(column_profiles),
        columns=column_profiles,
    )


def print_profile(profile: TableProfile) -> None:
    print(f"\n{'='*70}")
    print(f"  TABLE : {profile.table_name}")
    print(f"  FILE  : {profile.file_path}")
    print(f"  ROWS  : {profile.total_rows:,}   COLUMNS: {profile.total_columns}")
    print(f"{'='*70}")
    for col in profile.columns:
        hc_tag = "HIGH-CARDINALITY" if col.high_cardinality else "LOW-CARDINALITY"
        print(f"\n  [{col.name}]  ({col.dtype})  {hc_tag}")
        print(f"    distinct={col.distinct_count}  nulls={col.null_count}")
        print(f"    samples : {col.sample_values}")
        if col.min_val is not None:
            print(f"    numeric : min={col.min_val:.2f}  max={col.max_val:.2f}  avg={col.avg_val:.2f}")
    print()

# profile = load_and_profile("data\\Amazon Sale Report.csv")
# print_profile(profile)