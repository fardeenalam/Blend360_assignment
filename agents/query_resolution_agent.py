from __future__ import annotations

import os
from dotenv import load_dotenv
import json

load_dotenv()

from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage

from state import RetailAgenticState
from models import QueryResolutionOutput


# state for the agent
# commenting but can be used for testing the agent separately
# class QueryAgentState(TypedDict):
#     user_query: str
#     table_metadata: str
#     resolution: QueryResolutionOutput | None
#     messages: list[Any]
#     error: str | None




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

SYSTEM_PROMPT = """\
You are a Language-to-Query Resolution Agent.

Your task:
Convert a business user's natural language question into a structured query specification.

You are NOT allowed to:
- Generate SQL.
- Provide explanations or reasoning.
- Suggest optional filters.
- Add extra metrics not requested.
- Invent columns not in the metadata.

You MUST:
- Use only the exact column names defined in COLUMN_METADATA.
- Return strictly structured JSON matching the required schema.
- Map business terms to actual column names using dtype, statistics, and sample_values.
- Use lowercase aggregation names: sum, avg, count, min, max.
- Use exact column names (case-sensitive, including spaces if present).
- Add one to two lines of commentary on how the query maps to the metadata.

TABLE METADATA:
{table_metadata}

How to interpret metadata:
- dtype: physical type (DOUBLE, BIGINT, DATE, VARCHAR, BOOLEAN).
- distinct_count: cardinality of the column.
- null_count: rows containing NULL.
- high_cardinality: distinct_count is large relative to table size.
- sample_values: illustrative only - do NOT assume these are all possible values.

Interpretation rules:
- "revenue" or "sales" - numeric monetary column.
- "quantity" - numeric quantity column.
- time references (month, quarter, year) - date filters.
- trend request - group by time unit, include in dimensions.
- No grouping requested - single aggregated result.
- Only include dimensions explicitly requested or required for ranking/trend.

If the query intent is "ranking":
- If user uses words like "top", "highest", "best", apply descending sort.
- If user uses words like "worst", "lowest", "least", apply ascending sort.
- If the user explicitly specifies a number (e.g., "top 3", "top 10"), use that as the limit.
- If the user requests multiple items but does not specify a number (e.g., "top categories", "best segments"), default limit = 5.
- If the user clearly asks for a single item (e.g., "top category", "best product"), default limit = 1.

Ambiguity Resolution:
If multiple categorical columns match a business term:
1. Prefer the column with LOWER distinct_count.
2. Prefer broader grouping dimensions over granular identifiers.
3. Only select high-cardinality columns if the user explicitly points to it.
4. When in doubt, choose the broader business grouping dimension.

{chat_history_block}

Follow-up detection:
- If CONVERSATION HISTORY is present and the new query is short or references
  prior context ("same but", "now for", "add", "change", "filter by", etc.),
  treat it as a modification of the LAST QUERY SPEC in the history.
- For follow-ups: keep all fields from the last spec unchanged UNLESS the user
  explicitly modifies them. Only update the fields the user is asking to change.
- For fresh queries: ignore the history and produce a new spec from scratch.

Return structured output only.
"""

MAX_HISTORY = 5

def format_chat_history(chat_history: list[dict]) -> str:
    """
    Render chat history as a block for the resolution agent system prompt.
    For assistant messages we include both the natural language answer AND
    the last query_spec so the agent can treat follow-ups as modifications.
    """
    if not chat_history:
        return ""

    lines = ["CONVERSATION HISTORY (most recent last):"]
    last_spec = None

    for msg in chat_history:
        role = msg["role"].upper()
        content = msg.get("content", "")
        lines.append(f"  [{role}]: {content}")
        if msg["role"] == "assistant" and msg.get("query_spec"):
            last_spec = msg["query_spec"]

    if last_spec:
        lines.append(
            f"\n  LAST QUERY SPEC (modify only the fields the user is changing):\n"
            f"  {json.dumps(last_spec, indent=2)}"
        )

    return "\n".join(lines)

def trim_history(chat_history):
    max_msgs = MAX_HISTORY * 2
    return chat_history[-max_msgs:]


# Agent Node
def query_resolution_agent(state: RetailAgenticState) -> RetailAgenticState:
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

    chat_history_block = format_chat_history(state.get("chat_history", []))



    system_prompt = SYSTEM_PROMPT.format(
        table_metadata=state["table_metadata"],
        chat_history_block = chat_history_block
        )

    feedback = state.get("validation_feedback")

    user_content = f"""
User Query: {state["user_query"]}

Return the structured output now.
"""

    if feedback:
        user_content = (
            f"{state['user_query']}\n\n"
            f"[PREVIOUS ATTEMPT FAILED: {feedback}. Adjust query spec to fix this.]\n\n"
            "Return the structured output now."
        )
    
    try:
        result: QueryResolutionOutput = structured_llm.invoke([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
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
# def build_query_resolution_graph() -> StateGraph:
#     graph = StateGraph(RetailAgenticState)

#     graph.add_node("query_resolution", query_resolution_agent)
    
#     graph.add_edge(START, "query_resolution")
#     graph.add_edge("query_resolution", END)

#     return graph.compile()


# def run_query_resolution_agent(user_query: str, table_profile) -> QueryResolutionOutput:
#     metadata_str = build_metadata_context(table_profile)
#     # print(metadata_str)

#     graph = build_query_resolution_graph()

#     initial_state: RetailAgenticState = {
#         "user_query": user_query,
#         "table_metadata": metadata_str,
#         "resolution": None,
#         "messages": [],
#         "error": None,
#     }

#     final_state: RetailAgenticState = graph.invoke(initial_state)

#     if final_state.get("error"):
#         raise RuntimeError(f"Query Resolution Agent failed: {final_state['error']}")
    
#     return final_state["resolution"]



# def print_resolution(resolution: QueryResolutionOutput) -> None:
#     print("\n" + "=" * 70)
#     print("QUERY SPEC")
#     print("=" * 70)
#     print(resolution.model_dump_json(indent=2))
#     print("=" * 70 + "\n")