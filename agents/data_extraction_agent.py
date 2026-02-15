from __future__ import annotations

import os
from dotenv import load_dotenv
import json

load_dotenv()

from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage

from state import RetailAgenticState
from models import DataExtractionOutput


SYSTEM_PROMPT = """\
You are a SQL generation agent working with DuckDB.

You will receive:
- A structured query specification (intent, columns, aggregations, filters, sort, limit)
- The table name and schema metadata

Your job: write a single, complete, executable DuckDB SQL query that fulfils the spec.

Rules:
- Always double-quote column names (they may contain spaces or hyphens): "Order ID", "ship-city"
- Use the exact table name given — do not quote the table name itself
- For string filtering use ILIKE to handle mixed case in the data
- For counting orders use COUNT(DISTINCT "Order ID"), not COUNT(*)
- SUM() and AVG() ignore NULLs automatically — safe on nullable columns
- For date filtering use DuckDB DATE literals: DATE '2022-04-01'
- For boolean columns filter with = true or = false (no quotes)
- Do not add LIMIT unless the spec explicitly sets one
- Return only the SQL — no markdown, no explanation in the sql field
"""

def data_extraction_agent(state: RetailAgenticState) -> RetailAgenticState:
    """This agent generates sql query and its explanation using the resolution output"""

    api_key    = os.getenv("OPENAI_API_KEY")
    resolution = state.get("resolution")

    if not api_key:
        return {**state, "error": "OPENAI_API_KEY not set.",
                "messages": state["messages"] + [AIMessage(content="DataExtractionAgent: no API key")]}

    if not resolution:
        return {**state, "error": "data_extraction_agent: no resolution in state",
                "messages": state["messages"] + [AIMessage(content="DataExtractionAgent: no resolution")]}
    
    llm = ChatOpenAI(
        model = "gpt-5-mini",
        api_key=api_key,
        temperature=0
    )

    structured_llm = llm.with_structured_output(DataExtractionOutput)


    feedback = state.get("validation_feedback", "")
    retry_note = (
        f"\n\nPREVIOUS SQL ATTEMPT FAILED VALIDATION:\n{feedback}\n"
        f"Fix the SQL to address this issue."
        if feedback else ""
    )

    user_prompt = f"""Table name   : {state['table_name']}
Table metadata:
{state['table_metadata']}

Query specification:
{json.dumps(resolution.model_dump(), indent=2)}{retry_note}

Generate the DuckDB SQL query that fulfils this specification exactly.
"""
    
    try:
        response: DataExtractionOutput = structured_llm.invoke([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ])

        sql = response.sql.strip().removeprefix("```sql").removeprefix("```").removesuffix("```").strip()

        # Exceuting the sql query against the shared DuckDB connection
        result = state["db_con"].execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

        return {
            **state,
            "sql": sql,
            "rows": rows,
            "columns": columns,
            "error": None,
            "messages": state["messages"] + [
                AIMessage(content=f"DataExtractionAgent: SQL generated & executed -> {len(rows)} rows")
            ],
        }
        
    except Exception as e:
        return {
            **state,
            "sql":      "",
            "rows":     [],
            "columns":  [],
            "error":    f"data_extraction_agent failed: {e}",
            "messages": state["messages"] + [
                AIMessage(content=f"DataExtractionAgent: error - {e}")
            ],
        }