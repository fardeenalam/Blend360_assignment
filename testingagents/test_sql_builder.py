from __future__ import annotations

from typing import Any, List
from dataclasses import dataclass
from pathlib import Path
import duckdb


# -------------------------
# Your SQL Builder Code
# -------------------------

def _lit(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if value is None:
        return "NULL"
    return str(value)


def _q(col: str) -> str:
    return f'"{col}"'


_OP_MAP = {
    "equals":           lambda c, v: f"{c} = {_lit(v)}",
    "greater_than":     lambda c, v: f"{c} > {_lit(v)}",
    "less_than":        lambda c, v: f"{c} < {_lit(v)}",
    "greater_or_equal": lambda c, v: f"{c} >= {_lit(v)}",
    "less_or_equal":    lambda c, v: f"{c} <= {_lit(v)}",
    "in":               lambda c, v: f"{c} IN ({', '.join(_lit(x) for x in v)})",
    "between":          lambda c, v: f"{c} BETWEEN {_lit(v[0])} AND {_lit(v[1])}",
}


def build_sql(resolution, table_name: str) -> str:
    select_parts: list[str] = []

    if resolution.aggregations:
        for agg in resolution.aggregations:
            fn    = agg.function.upper()
            col   = _q(agg.column)
            alias = f'{fn.lower()}_{agg.column.lower().replace(" ", "_")}'
            if fn == "COUNT":
                select_parts.append(f'COUNT(DISTINCT {col}) AS "{alias}"')
            else:
                select_parts.append(f'{fn}({col}) AS "{alias}"')
        for dim in (resolution.dimensions or []):
            select_parts.append(_q(dim))
    else:
        for col in resolution.relevant_columns:
            select_parts.append(_q(col))

    sql = f"SELECT {', '.join(select_parts)}\nFROM {table_name}"

    if resolution.filters:
        conditions = []
        for f in resolution.filters:
            op_fn = _OP_MAP.get(f.operator)
            if op_fn:
                conditions.append(op_fn(_q(f.column), f.value))
        if conditions:
            sql += "\nWHERE " + "\n  AND ".join(conditions)

    if resolution.aggregations and resolution.dimensions:
        sql += "\nGROUP BY " + ", ".join(_q(d) for d in resolution.dimensions)

    if resolution.sort:
        order_parts = [f'{_q(s.column)} {s.direction.upper()}' for s in resolution.sort]
        sql += "\nORDER BY " + ", ".join(order_parts)

    if resolution.limit:
        sql += f"\nLIMIT {resolution.limit}"

    return sql


# -------------------------
# Mock Resolution Classes
# -------------------------

@dataclass
class Aggregation:
    column: str
    function: str


@dataclass
class Sort:
    column: str
    direction: str


@dataclass
class Resolution:
    intent: str
    relevant_columns: List[str]
    aggregations: List[Aggregation]
    dimensions: List[str]
    filters: List
    sort: List[Sort]
    limit: int
    comments: str


# -------------------------
# Create Resolution Object
# -------------------------

resolution = Resolution(
    intent="ranking",
    relevant_columns=["Category", "Amount"],
    aggregations=[
        Aggregation(column="Amount", function="sum")
    ],
    dimensions=["Category"],
    filters=[],
    sort=[
        Sort(column="Amount", direction="desc")
    ],
    limit=1,
    comments="Top performing category by total amount"
)


# -------------------------
# DuckDB Execution
# -------------------------

csv_path = Path("data/Amazon Sale Report.csv")

if not csv_path.exists():
    raise FileNotFoundError(f"CSV file not found at: {csv_path}")

table_name = csv_path.stem.lower().replace(" ", "_").replace("-", "_")

con = duckdb.connect(database=":memory:")

con.execute(
    f"""
    CREATE TABLE {table_name} AS
    SELECT * FROM read_csv_auto('{csv_path.resolve()}', header=true)
    """
)

total_rows = con.execute(
    f"SELECT COUNT(*) FROM {table_name}"
).fetchone()[0]

print(f"\nTotal rows loaded: {total_rows}")

# -------------------------
# Build SQL
# -------------------------

sql_query = build_sql(resolution, table_name)

print("\nGenerated SQL:\n")
print(sql_query)

# -------------------------
# Execute SQL
# -------------------------

result = con.execute(sql_query).fetchall()

print("\nQuery Result:\n")
print(result)
