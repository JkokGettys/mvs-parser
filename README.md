# MVS Parser Service

A standalone service for parsing Marshall Valuation Service PDF data and writing it to PostgreSQL.

## Overview

This service runs separately from the Bowery Webapp and is responsible for:
- Parsing MVS PDF files to extract multiplier and cost data
- Writing parsed data to the Railway PostgreSQL database
- Can be triggered manually or via scheduled jobs

## Architecture

```
MVS PDF Files
     │
     ▼
┌─────────────────────────┐
│  MVS Parser Service     │
│  (This service)         │
│  - Python parsers       │
│  - FastAPI endpoint     │
└───────────┬─────────────┘
            │ writes
            ▼
┌─────────────────────────┐
│  Railway PostgreSQL     │
│  - mvs_local_multipliers│
│  - mvs_current_cost_... │
│  - mvs_story_height_... │
│  - etc.                 │
└─────────────────────────┘
            │ reads
            ▼
┌─────────────────────────┐
│  Bowery Webapp          │
│  (read-only access)     │
└─────────────────────────┘
```

## Setup

### Environment Variables

```
MVS_DATABASE_URL=postgresql://postgres:PASSWORD@HOST:PORT/railway
```

### Local Development

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Run the service
uvicorn app.main:app --reload --port 8000
```

### Railway Deployment

1. Connect this repo to Railway
2. Set `MVS_DATABASE_URL` environment variable
3. Railway auto-detects Python and deploys

## API Endpoints

- `GET /health` - Health check
- `POST /parse/local-multipliers` - Parse and update local multipliers
- `POST /parse/current-cost` - Parse and update current cost multipliers
- `POST /parse/story-height` - Parse and update story height multipliers
- `POST /parse/floor-area-perimeter` - Parse and update floor area/perimeter multipliers
- `POST /parse/all` - Run all parsers

## Parsers

Each parser reads from a PDF file and writes structured data to PostgreSQL:

| Parser | Source | Target Table |
|--------|--------|--------------|
| `parse_local_multipliers.py` | Pages 719-724 | `mvs_local_multipliers` |
| `parse_current_cost.py` | Page 717 | `mvs_current_cost_multipliers` |
| `parse_story_height.py` | Page 90 | `mvs_story_height_multipliers` |
| `parse_floor_area_perimeter.py` | Page 90 | `mvs_floor_area_perimeter_multipliers` |
