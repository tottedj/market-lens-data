# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`market-lens-data` is a Python-based financial market data pipeline. It uses Supabase as the backend database/auth layer.

## Setup

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

Environment variables go in `.env` (currently empty). Expected keys include `SUPABASE_URL` and `SUPABASE_KEY`.

## Key Dependencies & Their Roles

- **`supabase`** — primary data store and auth (PostgreSQL via PostgREST, real-time via WebSockets)
- **`pydantic`** — data validation and schema modeling
- **`click`** — CLI entry points
- **`httpx` / `requests`** — HTTP clients for fetching market data
- **`tenacity`** — retry logic for external API calls
- **`rich`** — terminal output formatting
- **`strictyaml`** — configuration file parsing
