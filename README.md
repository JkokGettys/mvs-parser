# MVS Parser Service

A standalone service for parsing Marshall Valuation Service PDF data and writing it to PostgreSQL, with an admin UI for version management and parser orchestration.

## Overview

This service runs separately from the Bowery Webapp and is responsible for:
- **PDF version management** -- upload, store, activate, and track MVS PDF editions
- **PDF parsing** -- extract multiplier and cost data from MVS PDFs into PostgreSQL
- **Admin frontend** -- React/MUI dashboard for managing versions, running parsers, and validating data
- **Data API** -- read endpoints for the Bowery Webapp to query parsed data

## Architecture

```
┌───────────────────────────────────────────┐
│  Admin Frontend (React + MUI)             │
│  /admin/                                  │
│  - Version list with active tag           │
│  - PDF upload                             │
│  - Parser dashboard (run/status/validate) │
└───────────────┬───────────────────────────┘
                │ API calls
                ▼
┌───────────────────────────────────────────┐
│  FastAPI Backend                          │
│  - PDF version CRUD                       │
│  - Parser endpoints (17 parsers)          │
│  - Parse run tracking (per-parser status) │
│  - Validation checks                      │
│  - Data query endpoints                   │
└───────────────┬───────────────────────────┘
                │ writes
                ▼
┌───────────────────────────────────────────┐
│  Railway PostgreSQL                       │
│  - mvs_pdf_versions                       │
│  - mvs_parse_runs (per-parser tracking)   │
│  - mvs_local_multipliers                  │
│  - mvs_current_cost_multipliers           │
│  - mvs_story_height_multipliers           │
│  - mvs_floor_area_perimeter_multipliers   │
│  - mvs_sprinkler_costs                    │
│  - mvs_hvac_costs                         │
│  - mvs_base_cost_tables / _rows           │
│  - mvs_elevator_types / _costs            │
└───────────────┬───────────────────────────┘
                │ reads
                ▼
┌───────────────────────────────────────────┐
│  Bowery Webapp (read-only access)         │
└───────────────────────────────────────────┘
```

## Setup

### Environment Variables

```
MVS_DATABASE_URL=postgresql://postgres:PASSWORD@HOST:PORT/railway
MVS_PDF_STORAGE_PATH=/data/mvs-pdfs   # Railway Volume mount for PDF storage
```

### Local Development

```bash
# 1. Start the FastAPI backend
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# 2. Start the admin frontend (separate terminal)
cd frontend
npm install
npm run dev                    # Runs on http://localhost:3001/admin/
```

The Vite dev server proxies API calls to `localhost:8000` automatically.

### Railway Deployment

1. Connect this repo to Railway
2. Set `MVS_DATABASE_URL` and `MVS_PDF_STORAGE_PATH` environment variables
3. The Dockerfile builds the frontend and serves it from FastAPI at `/admin/`

## Admin Frontend

The admin UI is a React + MUI 5 app that matches the Bowery Webapp visual style (Nunito Sans font, `#4260D3` primary, `#2E4154` text).

### Pages

**Version List** (`/admin/`)
- Database summary stats bar
- Version table with active version highlighted
- Upload new PDF dialog (drag-and-drop area, version name, edition year, notes)
- Set active / delete actions per version

**Version Detail** (`/admin/versions/:id`)
- Version metadata header (name, year, size, hash, notes, active status)
- Parsing progress bar with completion percentage
- Parser groups organized by category:
  - Global Multipliers (local multipliers, current cost)
  - Story Height Multipliers (S11, S13, S14, S15)
  - Floor Area / Perimeter Multipliers (S11, S13, S14, S15)
  - Base Cost Tables (S11, S13, S14, S15)
  - Refinements & Equipment (sprinklers, HVAC, elevators)
- Each parser shows: status icon, label, description, record count, run/re-run button
- "Run All Parsers" button with auto-polling for progress
- Collapsible validation section with pass/fail per parser (expected vs actual record counts)

## API Endpoints

### Version Management
- `GET /pdf-versions` -- List all versions
- `GET /pdf-versions/active` -- Get active version
- `GET /pdf-versions/{id}` -- Get version details
- `POST /pdf-versions/upload` -- Upload new PDF
- `PATCH /pdf-versions/{id}/activate` -- Set version as active
- `DELETE /pdf-versions/{id}` -- Delete version (non-active only)

### Parser Execution
- `POST /parse-version/{id}/local-multipliers` -- Parse local multipliers
- `POST /parse-version/{id}/current-cost` -- Parse current cost multipliers
- `POST /parse-version/{id}/story-height?section=N` -- Parse story height (per section)
- `POST /parse-version/{id}/story-height/all-sections` -- Parse all story height sections
- `POST /parse-version/{id}/floor-area-perimeter?section=N` -- Parse FA/P (per section)
- `POST /parse-version/{id}/floor-area-perimeter/all-sections` -- Parse all FA/P sections
- `POST /parse-version/{id}/all` -- Run all parsers at once

### Admin
- `GET /parsers` -- List all known parsers (17 total)
- `GET /pdf-versions/{id}/parse-runs` -- Get per-parser status for a version
- `POST /pdf-versions/{id}/validate` -- Run validation checks
- `GET /stats` -- Database record count summary

### Data Query (used by Bowery Webapp)
- `GET /local-multipliers` -- Query local multipliers
- `GET /current-cost-multipliers` -- Query current cost multipliers
- `GET /story-height-multipliers?section=N` -- Query story height multipliers
- `GET /floor-area-perimeter-multipliers?section=N` -- Query FA/P multipliers
- `GET /sprinkler-costs?section=N` -- Query sprinkler costs
- `GET /hvac-costs?section=N` -- Query HVAC costs
- `GET /tables?section=N` -- Query base cost tables
- `GET /elevators` -- Query elevator types

## Parsers (17 Total)

| Parser | Section | Target Table |
|--------|---------|--------------|
| Local Multipliers | Global | `mvs_local_multipliers` |
| Current Cost | Global | `mvs_current_cost_multipliers` |
| Story Height S11-S15 | 11,13,14,15 | `mvs_story_height_multipliers` |
| Floor Area/Perimeter S11-S15 | 11,13,14,15 | `mvs_floor_area_perimeter_multipliers` |
| Base Cost Tables S11-S15 | 11,13,14,15 | `mvs_base_cost_tables` + `_rows` |
| Sprinklers | All | `mvs_sprinkler_costs` |
| HVAC | All | `mvs_hvac_costs` |
| Elevators | 58 | `mvs_elevator_types` + `_costs` |
