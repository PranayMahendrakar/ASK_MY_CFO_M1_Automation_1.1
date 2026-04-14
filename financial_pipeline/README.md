# ASK MY CFO — M1 Automation

A professional web GUI that extracts, parses, and standardizes Indian financial statements (Balance Sheet & P&L) from annual report PDFs into a clean Excel template.

## Architecture

The pipeline runs in 3 automated stages:

```
PDF Upload → [Stage 1: Page Detection] → [Stage 2: Table Extraction] → [Stage 3: AI Mapping] → Excel Report
```

| Stage | Module | What it does |
|-------|--------|-------------|
| 1 | `page_detector.py` | Scans the PDF to find Balance Sheet and P&L pages using title-anchoring + data-pattern scoring |
| 2 | `extract_tables.py` | Parses PDF layout into structured rows using header-anchored column detection |
| 3 | `bs_pl_mapper.py` | Maps line items to a standardized template using GPT-4o with BS validation + residual-based P&L balancing |

## Quick Start

### macOS / Linux
```bash
chmod +x run.sh
./run.sh
```

### Windows
```
run.bat
```

Then open **http://localhost:5000** in your browser.

### Manual Setup
```bash
pip install -r requirements.txt
python app.py
```

## Requirements

- **Python 3.9+**
- **OpenAI API key** (for GPT-4o mapping in Stage 3)
- Dependencies: Flask, pdfplumber, openpyxl, pandas, openai, pypdf, reportlab

## Usage

1. Open `http://localhost:5000`
2. Drag & drop one or more annual report PDFs
3. Enter your OpenAI API key
4. Click **Run Pipeline**
5. Watch real-time progress in the console
6. Download the generated Excel files:
   - `*_extracted.xlsx` — Raw extracted data (intermediate)
   - `*_Report.xlsx` — Final standardized template with all mappings

## Features

- **Drag & drop** PDF upload with multi-file support
- **Real-time SSE streaming** of pipeline logs
- **3-stage progress tracking** with visual pipeline indicator
- **BS auto-validation** with retry on imbalance
- **Residual F72 computation** guarantees P&L always balances
- **Standalone + Consolidated** detection and separate mapping
- **Professional Excel output** with formulas, formatting, and verification rows

## Project Structure

```
financial_pipeline/
├── app.py              # Flask web server + pipeline orchestrator
├── index.html          # Web GUI frontend
├── requirements.txt    # Python dependencies
├── run.sh              # Linux/macOS launcher
├── run.bat             # Windows launcher
├── modules/
│   ├── page_detector.py    # Stage 1: PDF page detection
│   ├── extract_tables.py   # Stage 2: Table extraction
│   └── bs_pl_mapper.py     # Stage 3: LLM mapping
├── uploads/            # Temporary upload storage (auto-created)
└── output/             # Generated output files (auto-created)
```
