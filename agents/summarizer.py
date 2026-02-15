from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

load_dotenv()


# Output model for the summarizer function
class SummaryQuery(BaseModel):
    title: str = Field(description="Short label for this metric, For example - 'Total Revenue")
    sql: str = Field(description = (
        "Complete executable DuckDB SQL. Rules: "
        "1. Double-quote all column names. "
        "2. Use the exact table name provided. "
        "3. No backticks. "
        "4. Use ILIKE for string filters. "
        "5. Alias every aggregated column with a readable name."
    ))

class SummaryQueryPlan(BaseModel):
    queries: list[SummaryQuery] = Field(
        description=(
            "8 to 12 SQL queries covering the most informative metrics for this dataset. "
            "Must include a mix of: overall totals, top-N rankings, time-based breakdowns, "
            "category/segment distributions, and any dataset-specific insights the metadata suggests."
        )
    )


SYSTEM_PROMPT = """\
You are a senior data analyst. Given a table's metadata (column names, types,
cardinality, sample values, numeric ranges) you decide which SQL queries
will produce the most insightful business summary.

Generate 8 to 12 queries covering:
- Overall totals (total revenue, total orders, total quantity)
- Top-N rankings (top 5 categories, top 5 regions/states, top 5 products if identifiable)
- Distributions across key categorical dimensions (status, channel, segment, type)
- Time-based breakdown if a date column exists (monthly or weekly trend)
- Any metric the column metadata uniquely suggests (B2B vs B2C split, fulfilment type, etc.)

Rules for SQL:
- Always double-quote column names
- Use the exact table name given
- Alias every aggregated value with a descriptive name
- LIMIT rankings to 5 rows
- For time trends, use DATE_TRUNC or STRFTIME to group by month
- Never invent column names - only use what appears in the metadata
"""


# Pass the metadata to the llm and let it decide the sql queries for various metrics.
def plan_summary_queries(metadata_str: str, table_name: str) -> list[SummaryQuery]:
    api_key = os.getenv("OPENAI_API_KEY")
    llm = ChatOpenAI(
        model="gpt-5-mini", 
        api_key=api_key, 
        temperature=0)
    structured = llm.with_structured_output(SummaryQueryPlan)

    prompt = f"""Table name: {table_name}

Metadata:
{metadata_str}

Generate the SQL queries that will power a comprehensive business summary of this dataset.
"""
    
    result: SummaryQueryPlan = structured.invoke([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": prompt},
    ])

    return result.queries


# Execute the generated sql queries against the the duckdb connection
def execute_summary_queries(queries: list[SummaryQuery], db_con: Any) -> list[dict]:
    results = []
    
    for q in queries:
        try:
            # removing any trailing fluff producd by the llm
            sql = q.sql.strip().removeprefix("```sql").removeprefix("```").removesuffix("```").strip()
            
            res = db_con.execute(sql)

            columns = [d[0] for d in res.description]
            rows = [dict(zip(columns, row)) for row in res.fetchall()]

            results.append({
                "title":   q.title,
                "sql":     sql,
                "columns": columns,
                "rows":    rows,
            })
        except Exception as e:
            results.append({
                "title":   q.title,
                "sql":     q.sql,
                "columns": [],
                "rows":    [],
                "error":   str(e),
            })

    return results


# Convert the metrics retrieved into a well formatted markdown report.
def results_to_text(results: list[dict]) -> str:
    sections = []
    for r in results:
        if r.get("error") or not r["rows"]:
            continue
        lines = [f"### {r['title']}"]
        header = " | ".join(r["columns"])
        sep    = " | ".join("---" for _ in r["columns"])
        lines += [header, sep]
        for row in r["rows"][:10]:
            lines.append(" | ".join(str(row.get(c, "")) for c in r["columns"]))
        sections.append("\n".join(lines))
    return "\n\n".join(sections)

FORMATTING_SYSTEM_PROMPT = """\
You are a senior business analyst writing an executive data summary.

Convert the query results below into a polished markdown report.

Structure:
## Executive Summary
2-3 sentences: what this dataset covers, scale, date range if known.

## Key Metrics
Bullet-point the most important top-level numbers.

## Top Performers
Rankings — categories, regions, products that lead.

## Breakdown & Distribution
How the data splits across key segments.

## Trends
Time-based patterns if available.

## Notable Observations
Any anomalies, outliers, or interesting patterns the data reveals.

Rules:
- Use real numbers from the query results — do not invent figures.
- Currency values are in INR unless stated otherwise.
- Be concise but specific. Avoid filler sentences.
- Use markdown tables where it adds clarity (5 rows max per table).
- Write for a business audience, not a technical one.
"""  

def format_markdown(results: list[dict], table_name: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    llm = ChatOpenAI(model="gpt-4o-mini", api_key=api_key, temperature=0.3)

    data_text = results_to_text(results)
    if not data_text.strip():
        return "# Summary\n\nNo data could be extracted from the uploaded file."

    prompt = f"""Dataset: {table_name}

Query results:
{data_text}

Write the business summary report in markdown.
"""
    response = llm.invoke([
        {"role": "system", "content": FORMATTING_SYSTEM_PROMPT},
        {"role": "user",   "content": prompt},
    ])
    return response.content.strip()


# Main entrypoint for the streamlit interface and the main thread
def generate_summary(metadata_str: str, table_name: str, db_con: Any) -> str:
    """
    Called once after file upload; result cached for further use
    """
    queries = plan_summary_queries(metadata_str, table_name)
    results = execute_summary_queries(queries, db_con)
    return format_markdown(results, table_name)