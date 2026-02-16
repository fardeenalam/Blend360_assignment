# Retail Insights Assistant — Technical Notes

A conversational, multi-agent analytics system for retail sales CSVs. Users upload a file, receive an auto-generated summary, and ask natural language questions answered by a LangGraph agent pipeline backed by DuckDB.

---

## Project Structure

```
├── app.py                      # Streamlit UI — entry point
├── main.py                     # CLI entry point (no UI)
├── graph.py                    # LangGraph graph definition and router
├── state.py                    # Shared TypedDict state (RetailAgenticState)
├── models.py                   # Pydantic output models for all agents
├── dataprocessing/
    ├── datalayer.py            # CSV ingestion and column profiling
├── agents/
    ├── summarizer.py               # Multi-query summarization pipeline
    ├── query_resolution_agent.py   # Language-to-query spec agent
    ├── data_extraction_agent.py    # SQL generation and execution agent
    ├── validation_agent.py         # Result validation and retry routing agent
    ├── formatter_agent.py          # Natural language answer generation agent
├── data/                       # Uploaded CSVs saved here (upload.csv)
└── requirements.txt
```

---

## Setup

### 1. Prerequisites

- Python 3.10+
- An OpenAI API key (`gpt-5-mini` is used across all agents due to limited resources)

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Environment

Create a `.env` file in the project root:

```
OPENAI_API_KEY=sk-...
```

### 4. Run

**Streamlit UI (recommended)**

```bash
streamlit run app.py
```

Upload a CSV via the file uploader. The system will profile the data, generate a summary, and open the chat.

**CLI**

```bash
python main.py --csv "data/Amazon Sale Report.csv"
```

Type queries interactively. Type `exit` to quit.

**Debug mode** — prints the generated SQL after each response:

```bash
DEBUG=1 streamlit run app.py
# or
DEBUG=1 python main.py --csv "data/Amazon Sale Report.csv"
```

---

## How It Works

### Data Layer (`datalayer.py`)

On upload, the CSV is loaded into an in-memory DuckDB table with `all_varchar=true` to avoid date/type parsing errors from inconsistent formats. A full column scan runs to build a `TableProfile`: dtype, distinct count, null count, min/max/avg, 5 sample values, and a high-cardinality flag per column. This profile is serialised into a metadata string injected into every agent prompt.

The session DuckDB connection (used by agents for querying) loads the same CSV with `ignore_errors=true, sample_size=-1` to preserve real types for numeric aggregations while tolerating malformed rows.

### Summarization (`summarizer.py`)

Runs once at upload, blocking the UI until complete. Three steps:

1. **Query Planner** — LLM reads the column metadata and generates 8–12 SQL queries covering totals, top-N rankings, segment distributions, and time trends.
2. **Execution** — all queries run against DuckDB; individual failures are skipped silently.
3. **Synthesis** — LLM receives all result tables and writes a structured markdown report (Executive Summary, Key Metrics, Top Performers, Breakdown, Trends, Notable Observations).

The result is cached in `st.session_state`. Typing `summary` in the chat returns it instantly without an LLM call.

### Agent Pipeline (`graph.py`)

A LangGraph `StateGraph` with four nodes sharing a single `RetailAgenticState` TypedDict:

```
query_resolution → data_extraction → validation ──→ formatter
       ↑                  ↑               │
       └──────────────────┴───── retry ───┘
```

| Agent | Role |
|---|---|
| **Query Resolution** | Converts natural language to a structured spec: intent, columns, aggregations, filters, dimensions, sort, limit |
| **Data Extraction** | Generates DuckDB SQL from the spec and executes it against the session table |
| **Validation** | Assesses whether the result answers the question; routes failed attempts back to the responsible agent |
| **Formatter** | Converts raw rows into a concise business-friendly answer |

### Retry Logic

The validation agent receives the original question, the query spec, the generated SQL, and up to 10 result rows. On failure it returns both a reason and a `route_to` field — `query_resolution` if the spec misunderstood the intent, `data_extraction` if the SQL was wrong. Each agent has an independent retry budget of 3 attempts. When both budgets are exhausted the formatter surfaces a graceful error.

### Conversational Memory

The last 5 turns of chat history are passed into every Query Resolution call. The most recent query spec is embedded in the system prompt so follow-up queries ("now for Maharashtra", "top 3 only", "break it down by month") modify only the changed fields rather than generating a new spec from scratch.

---

## Assumptions

- **Single-table CSV files only.** The system profiles and queries one table per session. Multi-table are not supported. We also assume that the data will be sanitized before loading.
- **OpenAI only.** All four agents and the summarizer use `gpt-5-mini` via `langchain-openai`. No other provider is wired in.
- **Currency is INR** unless the data explicitly states otherwise. This is hardcoded in the formatter and synthesizer prompts.
- **Column names are used as-is.** The metadata context passes exact column names to the LLM. If the CSV has ambiguous or poorly named columns the resolution agent may pick incorrectly.
- **Date columns treated as VARCHAR in profiling.** To handle inconsistent date formats across rows, the profiling connection loads all columns as strings. The session query connection uses `ignore_errors=true` and full-file sampling to preserve types where possible.

---

## Known Limitations

- **No persistent storage.** Everything lives in `st.session_state` and in-memory DuckDB. Closing the browser tab or restarting Streamlit wipes all history and requires re-uploading the file.
- **Summarization blocks the UI.** The summary pipeline runs synchronously during upload. On large files (100MB+) with slow API responses this can take 30–60 seconds before the chat opens. This is ONE TIME so once generated the summary is presented instantly when required.
- **LLM-generated SQL can be wrong.** The validation+retry loop catches most failures but is not guaranteed. Complex queries involving multiple aggregations, date arithmetic, or unusual column names are most likely to require retries or fail gracefully.
- **`sample_size=-1` on large files is slow.** Full-file type inference in DuckDB on a 65MB CSV is fast, but on files above a few hundred MB this could add meaningful load time. This is why for now under the assumption that data is under 2GB I have loaded the table into the RAM using `:memory:` for sub-second latency.
- **No streaming responses.** The formatter agent waits for the full LLM response before rendering. Long answers have visible latency. This is a limitation in streamlit and can be fixed using modern UI frameworks and `app.stream()` in LangGraph.
- **Chat history is capped at 10 messages (5 turns).** Older context is trimmed and unavailable for follow-up detection.

---

## Possible Improvements

### Scale
- Replace the in-memory DuckDB connection with BigQuery for datasets beyond available RAM.
- Run the summarization pipeline in a background thread so the chat opens immediately while the summary generates.
- Cache the `TableProfile` to disk so re-uploading the same file skips re-profiling.

### Agent Quality
- Add a schema agent that runs before query resolution to verify that referenced columns actually exist and suggest corrections before SQL is generated.
- Improve follow-up detection reliability by storing the full query spec history rather than just the most recent one.
- Add a confidence score to the validation agent output and escalate low-confidence passes to a second validation pass.
- Stream formatter output token-by-token for lower perceived latency.

### Observability
- Log every generated SQL query, validation result, and retry reason to a file or database for offline debugging and quality analysis.
- Expose a debug panel in the UI (currently gated behind `DEBUG=1`) showing SQL, agent trace, and retry count for each turn.

### UX
- Allow users to download the auto-generated summary as a PDF or Word document.
- Add suggested starter questions based on the column metadata, displayed when the chat first opens.
- Support multiple uploaded files in a single session for cross-table comparison.