from __future__ import annotations

import os
from typing import Any
from dotenv import load_dotenv

load_dotenv()

from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field
from typing_extensions import TypedDict


# Output model for the agent
class QueryResolutionOutput(BaseModel):
    reasoning: str = Field(
        description=(
            "Step-by-step reasoning: what the user is asking, which columns are "
            "relevant, how they map to SQL operations (filter / group / aggregate / sort)."
        )
    )
    relevant_columns: list[str] = Field(
        description=(
            "Exact column names from the table that are needed to answer this query. "
            "Must match the column names in the table metadata exactly."
        )
    )
    filters: list[str] = Field(
        description=(
            "Each entry is a plain-English filter condition, e.g. "
            "'Status = Shipped', 'Date BETWEEN 2022-04-01 AND 2022-06-30'. "
            "Empty list if no filtering is needed."
        )
    )
    aggregations: list[str] = Field(
        description=(
            "Each entry is a plain-English aggregation, e.g. "
            "'SUM(Amount) grouped by Category', 'COUNT(Order ID) grouped by ship-state'. "
            "Empty list if no aggregation is needed."
        )
    )
    sort_order: str = Field(
        description=(
            "How results should be ordered, e.g. 'DESC by total Amount'. "
            "Empty string if no ordering is specified."
        )
    )
    limit: int | None = Field(
        default=None,
        description="Row limit if the user asks for top-N results, else null."
    )
    sql_hint: str = Field(
        description=(
            "A near-complete SQL query draft (may have minor syntax gaps) that the "
            "Data Extraction Agent should use as its starting point. "
            "Always use double-quoted column names to handle spaces and special chars. "
            "Table name: amazon_sale_report."
        )
    )
    ambiguities: list[str] = Field(
        default_factory=list,
        description=(
            "Any ambiguous parts of the user query that required an assumption. "
            "e.g. 'User said revenue — assumed to mean the Amount column in INR.'"
        )
    )



# state for the agent
class QueryAgentState(TypedDict):
    user_query: str
    table_metadata: str
    resolution: QueryResolutionOutput | None
    messages: list[Any]
    error: str | None


_NUMERIC_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "REAL",
    "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT",
}

def build_metadata_context(table_profile) -> str:
    """
    Convert a TableProfile dataclass (from data_layer.load_and_profile) into
    a richly annotated string that the LLM can reason over.

    Every observation in this string is derived purely from the profiling data —
    no hardcoded column names, no dataset-specific assumptions.

    Structure produced:
        TABLE OVERVIEW
        COLUMN DETAILS  (one block per column with all stats + inferred hints)
        SQL RULES       (generic rules that apply to any table loaded this way)
    """
    col_lines = []

    for col in table_profile.columns:
        dtype_upper = col.dtype.upper().split("(")[0].strip()
        is_numeric  = dtype_upper in _NUMERIC_TYPES
        is_date     = "DATE" in dtype_upper or "TIME" in dtype_upper
        is_bool     = "BOOL" in dtype_upper
        null_pct    = round(col.null_count / col.total_rows * 100, 1) if col.total_rows else 0
        has_nulls   = col.null_count > 0

        block = [f'  Column "{col.name}"']
        block.append(f"    type          : {col.dtype}")
        block.append(f"    distinct vals : {col.distinct_count:,}  ({'HIGH-CARDINALITY — avoid SELECT DISTINCT without LIMIT' if col.high_cardinality else 'LOW-CARDINALITY — safe to enumerate'})")

        if has_nulls:
            block.append(f"    nulls         : {col.null_count:,} rows ({null_pct}% of total) — exclude nulls in aggregations")
        else:
            block.append(f"    nulls         : none")

        # Sample values — truncate long strings so prompt stays clean
        truncated_samples = []
        for v in col.sample_values[:5]:
            s = str(v)
            truncated_samples.append(s[:60] + "…" if len(s) > 60 else s)
        block.append(f"    sample values : [{', '.join(truncated_samples)}]")

        # Numeric range
        if is_numeric and col.min_val is not None:
            block.append(f"    numeric range : min={col.min_val:.2f}  max={col.max_val:.2f}  avg={col.avg_val:.2f}")

        # --- Inferred query hints, derived entirely from the stats ---

        hints = []

        # Identifier-like: high cardinality VARCHAR close to total row count
        if not is_numeric and not is_date and col.distinct_count > (col.total_rows * 0.5):
            hints.append("likely a unique identifier — use for COUNT(DISTINCT ...) not GROUP BY")

        # Good grouping dimension: low-cardinality non-numeric
        if not is_numeric and not is_date and not is_bool and col.distinct_count <= 50:
            hints.append("good GROUP BY dimension — low cardinality, safe to enumerate")

        # Numeric measure: suggest aggregations
        if is_numeric and not col.name.lower() in ("index",):
            hints.append("numeric measure — suitable for SUM / AVG / MIN / MAX aggregations")

        # Date column
        if is_date:
            hints.append("date column — use for time filtering, BETWEEN, DATE_TRUNC, date_part()")

        # Boolean
        if is_bool:
            hints.append("boolean — filter with = true / = false, or use SUM(CASE WHEN ... END) for counts")

        # High null rate warning
        if null_pct >= 20:
            hints.append(f"high null rate ({null_pct}%) — results may be skewed if nulls are not handled")

        # Mixed-case string values (detect by checking if sample set has both upper and lower chars)
        if not is_numeric and not is_date:
            samples_str = " ".join(str(v) for v in col.sample_values)
            if samples_str != samples_str.upper() and samples_str != samples_str.lower():
                hints.append("mixed-case string values — use LOWER() or ILIKE for reliable filtering")

        if hints:
            block.append(f"    query hints   : {' | '.join(hints)}")

        col_lines.append("\n".join(block))

    metadata = f"""TABLE OVERVIEW
  name        : {table_profile.table_name}
  total rows  : {table_profile.total_rows:,}
  total cols  : {table_profile.total_columns}

COLUMN DETAILS
{chr(10).join(col_lines)}

SQL RULES (always apply)
  1. Wrap every column name in double quotes — names may contain spaces or hyphens.
  2. Table name to use in FROM clause: {table_profile.table_name}
  3. For counting unique orders/entities prefer COUNT(DISTINCT "col") over COUNT(*).
  4. SUM() and AVG() automatically ignore NULLs — safe to use on nullable numeric cols.
  5. For string columns with mixed case use LOWER("col") = LOWER('value') or ILIKE.
  6. For date filtering use DATE literals: DATE '2022-04-01' or BETWEEN DATE '...' AND DATE '...'.
  7. For boolean columns filter with = true or = false (no quotes).
"""
    return metadata


SYSTEM_PROMPT = """\
You are the Language-to-Query Resolution Agent in a retail analytics pipeline.

Your job is to read a natural language question from a business user and translate
it into a precise, structured query plan. You do NOT execute SQL — you produce a
plan that the Data Extraction Agent will execute.

You must base your entire response on the TABLE METADATA below.
Do not assume any column names, values, or table structure beyond what is provided.

{table_metadata}

When resolving the query:
- Map the user's intent to the exact column names shown above.
- Use the query hints and null information to choose safe SQL patterns.
- If the user's question is ambiguous (e.g. "sales" could mean Amount or Qty),
  pick the most reasonable interpretation and record it in ambiguities.
- Always double-quote column names in the sql_hint.
"""


# Agent Node
def query_resolution_agent(state: QueryAgentState) -> QueryAgentState:
    """
    Language-to-Query Resolution Agent.

    Reads the user query + table metadata from state and produces
    a structured QueryResolutionOutput.
    """
    openai_key = os.getenv("OPENAI_API_KEY")

    if not openai_key:
        return {
            **state,
            "error": "OPENAI_API_KEY environment variable not set.",
            "messages": state["messages"] + [AIMessage(content="QueryResolutionAgent: failed — no API key")],
        }
    
    llm = ChatOpenAI(
        model = "gpt-5-mini",
        api_key=openai_key,
        temperature=0
    )

    structured_llm = llm.with_structured_output(QueryResolutionOutput)

    system_prompt = SYSTEM_PROMPT.format(table_metadata=state["table_metadata"])

    prompt = f"""
Resolve the following user query into a structured query plan.

User Query: {state["user_query"]}

Steps:
1. Identify the business intent (what metric / dimension / time range is the user after?).
2. Map intent to exact column names from the metadata above.
3. Identify any filters (WHERE conditions), aggregations (GROUP BY + aggregate functions),
   and sort order.
4. Draft a SQL query the Data Extraction Agent can execute with minimal modification.
5. List any assumptions you had to make in the ambiguities field.

Return the structured output now.
"""
    
    try:
        result: QueryResolutionOutput = structured_llm.invoke([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ])

        return {
            **state,
            "resolution": result,
            "error": None,
            "messages": state["messages"] + [
                AIMessage(content=f"QueryResolutionAgent: resolved query → {len(result.relevant_columns)} columns, sql_hint ready")
            ]
        }
    
    except Exception as exc:
        return {
            **state,
            "error": f"query_resolution_agent failed: {exc}",
            "messages": state["messages"] + [AIMessage(content=f"QueryResolutionAgent: error — {exc}")],
        }
    



# testing the agent
def build_query_resolution_graph() -> StateGraph:
    graph = StateGraph(QueryAgentState)

    graph.add_node("query_resolution", query_resolution_agent)
    
    graph.add_edge(START, "query_resolution")
    graph.add_edge("query_resolution", END)

    return graph.compile()


def run_query_resolution_agent(user_query: str, table_profile) -> QueryResolutionOutput:
    metadata_str = build_metadata_context(table_profile)

    graph = build_query_resolution_graph()

    initial_state: QueryAgentState = {
        "user_query": user_query,
        "table_metadata": metadata_str,
        "resolution": None,
        "messages": [],
        "error": None,
    }

    final_state: QueryAgentState = graph.invoke(initial_state)

    if final_state.get("error"):
        raise RuntimeError(f"Query Resolution Agent failed: {final_state['error']}")
    
    return final_state["resolution"]



def print_resolution(resolution: QueryResolutionOutput) -> None:
    sep = "=" * 70
    print(f"\n{sep}")
    print("  QUERY RESOLUTION OUTPUT")
    print(sep)
    print(f"\n  Reasoning:\n    {resolution.reasoning}\n")
    print(f"  Relevant columns : {resolution.relevant_columns}")
    print(f"  Filters          : {resolution.filters if resolution.filters else '(none)'}")
    print(f"  Aggregations     : {resolution.aggregations if resolution.aggregations else '(none)'}")
    print(f"  Sort order       : {resolution.sort_order or '(none)'}")
    print(f"  Limit            : {resolution.limit or '(none)'}")
    if resolution.ambiguities:
        print(f"\n  ⚠ Ambiguities:")
        for a in resolution.ambiguities:
            print(f"    - {a}")
    print(f"\n  SQL hint:\n")
    for line in resolution.sql_hint.splitlines():
        print(f"    {line}")
    print(f"\n{sep}\n")