from __future__ import annotations

from typing import Any

from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph

load_dotenv()

from state import RetailAgenticState
from agents.query_resolution_agent import query_resolution_agent
from agents.data_extraction_agent import data_extraction_agent
from agents.validation_agent import validation_agent 
from agents.formatter_agent import formatter_agent 

MAX_RETRIES_RESOLUTION = 3
MAX_RETRIES_EXTRACTION = 3

def validation_router(state: RetailAgenticState) -> str:
    if state.get("validation_passed"):
        return "formatter"
    
    route = state.get("route_to", "data_extraction")
    res_used = state.get("resolution_retry_count", 0)
    ext_used = state.get("extraction_retry_count", 0)

    if route == "query_resolution" and res_used < MAX_RETRIES_RESOLUTION:
        return "query_resolution"
    if route == "data_extraction" and ext_used < MAX_RETRIES_EXTRACTION:
        return "data_extraction"
    
    return "formatter"


def build_graph() -> Any:
    graph = StateGraph(RetailAgenticState)

    graph.add_node("query_resolution", query_resolution_agent)
    graph.add_node("data_extraction", data_extraction_agent)
    graph.add_node("validation", validation_agent)
    graph.add_node("formatter", formatter_agent)

    graph.add_edge(START, "query_resolution")
    graph.add_edge("query_resolution", "data_extraction")
    graph.add_edge("data_extraction", "validation")
    graph.add_conditional_edges(
        "validation",
        validation_router,
        {
            "query_resolution": "query_resolution",
            "data_extraction": "data_extraction",
            "formatter": "formatter",
        }
    )
    graph.add_edge("formatter", END)

    return graph.compile()