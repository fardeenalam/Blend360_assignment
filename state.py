from typing_extensions import TypedDict
from typing import Any
from models import QueryResolutionOutput


class RetailAgenticState(TypedDict):
    user_query: str
    table_metadata: str
    resolution: QueryResolutionOutput | None
    messages: list[Any]
    error: str | None

    db_con: Any

    sql: str
    rows: list[dict]
    columns: list[str]

    table_name: str

    chat_history: list[dict] 

    validation_passed: bool
    validation_reason: str
    validation_feedback: str   
    route_to: str   
    resolution_retry_count: int   
    extraction_retry_count: int

    final_answer: str
