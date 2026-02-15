from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage

from state import RetailAgenticState


def rows_to_text(rows: list[dict], columns: list[str], max_rows: int = 20) -> str:
    """tab-separated string for the prompt and limit rows to 20 for token savings"""
    if not rows:
        return "(no data)"
    lines = ["\t".join(str(c) for c in columns)]
    for row in rows[:max_rows]:
        lines.append("\t".join(str(row.get(c, "")) for c in columns))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows)")
    return "\n".join(lines)


FORMATTER_SYSTEM_PROMPT = """\
You are a data analyst presenting query results to a business user.

Convert the structured query results into a clear, concise natural language summary.

Rules:
- Lead with the direct answer to the user's question.
- Mention the top finding first (highest value, trend, etc.).
- If there are multiple rows, summarise the key pattern.
  Do not list every row verbatim unless there are 5 or fewer rows.
- Use business-friendly language. Avoid SQL terms like GROUP BY or SUM.
- Currency values are in INR unless stated otherwise.
- Keep the response to 3-5 sentences maximum.
- Do not suggest further analysis unless the data clearly warrants it.
"""


def formatter_agent(state: RetailAgenticState) -> RetailAgenticState:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {**state, "final_answer": "Error: OPENAI_API_KEY not set.", "error": "no API key"}

    rows    = state.get("rows", [])
    columns = state.get("columns", [])

    # Retries exhausted with no valid rows — surface the failure reason cleanly
    if not state.get("validation_passed") and not rows:
        reason = state.get("validation_reason", "Unknown error.")
        return {
            **state,
            "final_answer": f"I wasn't able to answer that. {reason}",
            "messages":     state["messages"] + [AIMessage(content="FormatterAgent: error answer")],
        }

    data_text = rows_to_text(rows, columns)
    prompt = (
        f"User's question: {state['user_query']}\n\n"
        f"SQL executed:\n{state.get('sql', '')}\n\n"
        f"Query results:\n{data_text}\n\n"
        f"Write a clear, business-friendly answer based on these results."
    )

    try:
        llm = ChatOpenAI(model="gpt-4o-mini", api_key=api_key, temperature=0.3)
        response = llm.invoke([
            {"role": "system", "content": FORMATTER_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ])
        return {
            **state,
            "final_answer": response.content.strip(),
            "error": None,
            "messages": state["messages"] + [AIMessage(content="FormatterAgent: answer generated")],
        }
    except Exception as exc:
        return {
            **state,
            "final_answer": f"Results ({len(rows)} rows):\n{data_text}",
            "error": f"formatter LLM failed: {exc}",
            "messages": state["messages"] + [AIMessage(content=f"FormatterAgent: fallback — {exc}")],
        }