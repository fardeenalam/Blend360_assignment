from pydantic import BaseModel, Field
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


class DataExtractionOutput(BaseModel):
    sql: str = Field(
        description=(
            "A complete, executable DuckDB SQL query. "
            "Rules: "
            "1. Always wrap column names in double quotes. "
            "2. Use the exact table name provided. "
            "3. Do not use backticks. "
            "4. For string comparisons use ILIKE to handle case sensitivity. "
            "5. Return only the SQL string, nothing else."
        )
    )
    explanation: str = Field(
        description="One sentence explaining what this SQL does."
    )


class ValidationOutput(BaseModel):
    passed: bool = Field(
        description="True if the result correctly answers the user's question, False otherwise."
    )
    reason: str = Field(
        description=(
            "If passed=True: brief confirmation of what the result shows. "
            "If passed=False: specific, actionable explanation of what is wrong."
        )
    )
    route_to: str = Field(
        description=(
            "Only relevant when passed=False. "
            "Set to 'query_resolution' when the problem is with how the question was interpreted - "
            "wrong columns chosen, wrong intent detected, wrong grouping dimensions, "
            "wrong aggregation type for the question asked. "
            "Set to 'data_extraction' when the intent was understood correctly but the SQL is wrong - "
            "syntax error, wrong operator, missing WHERE clause, bad JOIN, wrong LIMIT, "
            "empty result due to filter value mismatch, all-zero aggregation. "
            "When passed=True set this to ''."
        )
    )
