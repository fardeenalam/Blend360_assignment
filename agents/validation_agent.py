from __future__ import annotations

import os
from dotenv import load_dotenv
import json

load_dotenv()

from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage

from state import RetailAgenticState
from models import ValidationOutput


SYSTEM_PROMPT = """\
You are a data validation agent. Your job is to verify that a SQL query result
correctly and completely answers a user's business question.

You will receive:
- The user's original question
- The SQL that was executed
- A sample of the result rows (up to 10)
- The number of rows returned

Evaluate and return:
- passed: true only if the result makes sense as an answer to the question
- reason: if passed, briefly confirm what the result shows
          if failed, explain specifically what is wrong and how the SQL should be fixed

Fail conditions to check:
1. Result is empty (0 rows) - filters too strict or wrong column values
2. All numeric values in the result are 0 or NULL - wrong column or bad filter
3. The SQL clearly queries the wrong columns for the question asked
4. Result columns do not match what the user asked for
5. An obvious SQL error was propagated

If the result looks reasonable (even if imperfect), pass it.
Do not fail on minor formatting or ordering issues.
"""

def rows_to_text(rows: list[dict], columns: list[str], max_rows: int = 20) -> str:
    """Serialise result rows into a compact tab-separated string for the prompt."""
    if not rows:
        return "(no data)"
    lines = ["\t".join(str(c) for c in columns)]
    for row in rows[:max_rows]:
        lines.append("\t".join(str(row.get(c, "")) for c in columns))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows)")
    return "\n".join(lines)

def validation_agent(state: RetailAgenticState) -> RetailAgenticState:
    """Validates the output of the previous agents"""

    api_key    = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {**state, "error": "OPENAI_API_KEY not set.",
                "messages": state["messages"] + [AIMessage(content="DataExtractionAgent: no API key")]}
    
    if state.get("error"):
        reason = f"SQL execution failed: {state['error']}"
        ext_retries = state.get("extraction_retry_count", 0)
        return {
            **state,
            "validation_passed": False,
            "validation_reason": reason,
            "validation_feedback": reason,
            "route_to": "data_extraction",
            "extraction_retry_count": ext_retries + 1,
            "messages": state["messages"] + [
                AIMessage(content=f"ValidationAgent: FAIL SQL error → retry data_extraction")
            ],
        }

    resolution = state.get("resolution")
    rows = state.get("rows", [])
    columns = state.get("columns", [])
    sql = state.get("sql", "")

    sample_text = rows_to_text(rows[:10], columns) if rows else "(no rows returned)"
    
    prompt = f"""
    User's question: {state['user_query']}

Query specification (from resolution agent):
{json.dumps(resolution.model_dump(), indent=2)}

SQL generated (from data extraction agent):
{sql}

Rows returned: {len(rows)}

Result sample (up to 10 rows):
{sample_text}

Assess whether this result correctly answers the user's question.
If it does not, identify whether the problem is in the query spec (route to query_resolution)
or in the SQL generation (route to data_extraction).
"""

    llm = ChatOpenAI(
        model = "gpt-5-mini",
        api_key=api_key,
        temperature=0
    )

    structured_llm = llm.with_structured_output(ValidationOutput)

    try:
        response: ValidationOutput = structured_llm.invoke([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ])

        if response.passed:
            return {
                **state,
                "validation_passed": True,
                "validation_reason": response.reason,
                "validation_feedback": "",
                "route_to": "",
                "error": None,
                "messages": state["messages"] + [
                    AIMessage(content=f"ValidationAgent: PASS - {response.reason[:80]}")
                ],
            }
        else:
            route = response.route_to if response.route_to in ("query_resolution", "data_extraction") else "data_extraction"
            res_retries = state.get("resolution_retry_count", 0)
            ext_retries = state.get("extraction_retry_count", 0)
            return {
                **state,
                "validation_passed": False,
                "validation_reason": response.reason,
                "validation_feedback": response.reason,
                "route_to": route,
                "resolution_retry_count": res_retries + (1 if route == "query_resolution" else 0),
                "extraction_retry_count": ext_retries + (1 if route == "data_extraction"  else 0),
                "messages": state["messages"] + [
                    AIMessage(content=f"ValidationAgent: FAIL -> retry {route} - {response.reason[:80]}")
                ],
            }
    
    except Exception as exc:
        return {
            **state,
            "validation_passed": True,
            "validation_reason": f"Validation LLM failed ({exc}), passing through.",
            "validation_feedback": "",
            "messages": state["messages"] + [
                AIMessage(content=f"ValidationAgent: LLM error, passing through — {exc}")
            ],
        }

