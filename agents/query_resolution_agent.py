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
from typing import List, Union


# Output model for the agent
class FilterCondition(BaseModel):
    column: str = Field(description=("Exact column name from COLUMN_METADATA. Must match case and spelling exactly."))
    operator: str = Field(description=("Filtering operator. Allowed values 'equals', 'greater_than', 'less_than', 'greater_or_equal', 'less_or_equal', 'in', 'between'."))
    value: Union[
        str, int, float, bool, List[Union[str, int, float]]
    ] = Field(
        description=(
            "Literal value used for filtering. "
            "- For equals / greater_than / less_than: provide a single value. "
            "- For 'in': provide a list of values. "
            "- For 'between': provide a list of exactly two values [start, end]. "
            "Do NOT include SQL syntax. Only raw values."
        )
    )

class AggregationSpec(BaseModel):
    column: str = Field(description="Exact column name.")
    function: str = Field(description="sum, avg, count, min, max")


class SortSpec(BaseModel):
    column: str = Field(description="Exact column name ONLY. Use the raw column name.")
    direction: str = Field(description="asc or desc")

class QueryResolutionOutput(BaseModel):
    intent: str = Field(
        description="Type of query. Examples: 'aggregation', 'trend', 'ranking', 'filter_only'."
    )
    relevant_columns: list[str] = Field(
        description=(
            "Exact column names from the table that are needed to answer this query. "
            "Must match the column names in the table metadata exactly."
        )
    )
    aggregations: list[AggregationSpec] | None
    dimensions: list[str] = Field(
        default_factory=list,
        description="Exact column names used for GROUP BY."
    )
    filters: list[FilterCondition] = Field(default_factory=list)
    sort: list[SortSpec] | None
    limit: int | None = Field(
        default=None,
        description="Top-N limit if requested."
    )
    comments: str = Field(description= "A small content on how the user query maps to the table metadata")



# state for the agent
class QueryAgentState(TypedDict):
    user_query: str
    table_metadata: str
    resolution: QueryResolutionOutput | None
    messages: list[Any]
    error: str | None




def build_metadata_context(table_profile) -> str:
    """
    Build a structured metadata block for the system_prompt
    """

    column_blocks = []

    for col in table_profile.columns:
        block = {
            "name": col.name,
            "dtype": col.dtype,
            "distinct_count": col.distinct_count,
            "null_count": col.null_count,
            "high_cardinality": col.high_cardinality,
            "sample_values": col.sample_values[:5],
            "min_val": col.min_val,
            "max_val": col.max_val,
            "avg_val": col.avg_val,
        }

        column_blocks.append(block)

    return f"""
TABLE_NAME: {table_profile.table_name}
TOTAL_ROWS: {table_profile.total_rows}

COLUMN_METADATA:
{column_blocks}

IMPORTANT:
- sample_values are illustrative examples only.
- sample_values do NOT represent all possible values in the column.
- Use dtype and distinct_count to reason about metrics vs dimensions.
"""

SYSTEM_PROMPT = """
You are a Language-to-Query Resolution Agent.

Your task:
Convert a business user's natural language question into a structured query specification.

You are NOT allowed to:
- Generate SQL.
- Provide explanations.
- Provide reasoning.
- Suggest optional filters.
- Add extra metrics not requested.
- Invent columns that are not in the metadata.

You MUST:
- Use only the exact column names defined in COLUMN_METADATA.
- Return strictly structured JSON matching the required schema.
- Map business terms to actual column names using dtype, statistics, and sample_values.
- Use lowercase aggregation names: sum, avg, count, min, max.
- Use exact column names (case-sensitive, including spaces if present).
- Add a very minimal commentary, very small this can be one to two lines maximum inferring how the user query maps to the table metadata.

TABLE METADATA:
{table_metadata}

How to interpret metadata:

- dtype indicates the physical type (e.g., DOUBLE, BIGINT, DATE, VARCHAR, BOOLEAN).
- distinct_count indicates cardinality.
- null_count indicates how many rows contain NULL.
- high_cardinality indicates whether distinct_count is large relative to table size.
- sample_values are illustrative examples only.
  They DO NOT represent the full domain of the column.
  Do NOT assume these are the only possible values.

Interpretation rules:

- If the user says "revenue" or "sales", map to a numeric monetary column.
- If the user says "quantity", map to numeric quantity column.
- If the user asks for "top" or "highest", apply descending sort and limit 1.
- If the user references time (month, quarter, year), add appropriate time filters.
- If user asks for trend, group by time unit and include it in dimensions.
- If no grouping is requested, return a single aggregated result.
- Only include dimensions explicitly requested or required for ranking/trend.

Ambiguity Resolution Rule:
If multiple categorical columns could match a business term:
1. Prefer the column with LOWER distinct_count.
2. Prefer broader grouping dimensions over granular identifiers.
3. Only select high-cardinality columns if the user explicitly asks or the user query strongly points to it.
4. When in doubt, choose the broader business grouping dimension.

Return structured output only.
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
User Query: {state["user_query"]}

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
    # print(metadata_str)

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
    print("\n" + "=" * 70)
    print("QUERY SPEC")
    print("=" * 70)
    print(resolution.model_dump_json(indent=2))
    print("=" * 70 + "\n")