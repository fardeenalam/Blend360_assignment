# Retail Insights – Agentic CSV Analytics Assistant

Retail Insights is an AI-powered analytics assistant that allows users to upload a CSV dataset and ask business questions in natural language. The system profiles the dataset, converts queries into structured specifications, generates validated DuckDB SQL using LLMs, executes it in-memory, and returns concise, business-friendly answers. It supports conversational follow-ups and automatically generates an executive summary when a dataset is uploaded.

---

# Setup Guide

## 1. Clone the Repository

```bash
git clone https://github.com/fardeenalam/Blend360_assignment
cd Blend360_assignment
```

## 2. Create a Virtual Environment

### Mac / Linux

```bash
python -m venv venv
source venv/bin/activate
```
### Windows

```bash
python -m venv venv
venv\Scripts\activate
```

## 3. Install Dependencies

pip install the requirements.txt file

```bash
pip install -r requirements.txt
```

Otherwise install manually:

```bash
pip install langchain langgraph langchain-openai duckdb streamlit python-dotenv pydantic
```

## 4. Set OpenAI API Key

Create a `.env` file in the project root:

```
OPENAI_API_KEY=your_api_key_here
```

## 5. Run the Application

```bash
streamlit run app.py
```

Open the local URL shown in the terminal, upload a CSV file, and start querying your data.

---

# How It Works

The system is built as a multi-agent pipeline using LangGraph. Each agent is responsible for a specific step in the reasoning and execution process.

## End-to-End Flow

1. User uploads a CSV file
2. The dataset is profiled and metadata is generated
3. An executive summary is automatically created
4. User asks a natural language question
5. Query Resolution Agent converts it into a structured query specification
6. Data Extraction Agent generates and executes SQL
7. Validation Agent ensures correctness and handles retries
8. Formatter Agent produces a business-friendly answer

---

# Core Components

## Query Resolution Agent
- Converts natural language into a structured query specification
- Determines intent such as aggregation, ranking, filtering, or trend
- Uses metadata and conversation history
- Supports follow-up query modifications

## Data Extraction Agent
- Generates DuckDB SQL using structured LLM output
- Enforces strict SQL formatting rules
- Executes queries against a shared in-memory DuckDB connection
- Returns structured results (rows and columns)

## Validation Agent
- Detects SQL execution failures
- Routes back to resolution or extraction when needed
- Applies retry limits to avoid infinite loops

## Formatter Agent
- Converts structured query results into concise business summaries
- Avoids SQL terminology in final output
- Produces readable answers for non-technical users

## Summary Generator
- Automatically generates 8–12 insightful SQL queries on dataset upload
- Produces an executive-level markdown summary
- Covers totals, rankings, trends, and distributions

---

# Features

- Natural language to SQL conversion
- Structured LLM outputs for reliability
- Validation and retry routing
- Metadata-driven reasoning
- In-memory DuckDB execution
- Conversational context support
- Executive summary generation
- Streamlit-based interactive UI

---

# Example Questions

- What is the total revenue?
- Which category performed best?
- Show top 5 states by sales.
- How did revenue trend monthly?
- Same as above but only for 2022.

---

# Project Structure

```
.
├── app.py
├── graph.py
├── state.py
├── models.py
├── agents/
│   ├── query_resolution_agent.py
│   ├── data_extraction_agent.py
│   ├── validation_agent.py
│   ├── formatter_agent.py
│   └── summarizer.py
├── dataprocessing/
│   └── datalayer.py
└── data/
```

---

# Design Principles

- Clear separation between orchestration and agent logic
- Structured outputs to reduce hallucinations
- Metadata grounding for schema safety
- Controlled retries for robustness
- Business-oriented output formatting

---

# Future Improvements

- Add visualizations in responses
- Add query caching
- Add authentication and user sessions
- Add production logging
- Add usage monitoring

---


