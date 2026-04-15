# =============================================
# Robust Financial PDF Extraction Pipeline
# =============================================
# Designed for heterogeneous financial reports
# Extract -> Score -> Filter -> Normalize -> Store

import pandas as pd
import numpy as np

# =============================
# 1. EXTRACTION BACKENDS
# =============================

def extract_with_camelot(path):
    import camelot
    tables = camelot.read_pdf(path, pages="all", flavor="stream")
    return [t.df for t in tables]


def extract_with_pdfplumber(path):
    import pdfplumber
    tables = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            extracted = page.extract_tables()
            for t in extracted:
                if t:
                    tables.append(pd.DataFrame(t))
    return tables


def extract_with_layoutparser(path):
    # Optional: requires heavy setup
    try:
        from spacy_layout import spaCyLayout
        import spacy
        nlp = spacy.blank("en")
        layout = spaCyLayout(nlp)

        doc = layout(path)
        tables = []

        for table in doc._.tables:
            rows = [[cell.text for cell in row] for row in table]
            tables.append(pd.DataFrame(rows))

        return tables
    except Exception:
        return []


EXTRACTORS = [
    extract_with_camelot,
    extract_with_pdfplumber,
    extract_with_layoutparser,
]


def extract_all_tables(pdf_path):
    tables = []

    for extractor in EXTRACTORS:
        try:
            result = extractor(pdf_path)
            tables.extend(result)
        except Exception:
            continue

    return tables


# =============================
# 2. TABLE SCORING
# =============================

FINANCIAL_KEYWORDS = [
    "assets", "liabilities", "equity",
    "revenue", "profit", "cash", "income",
    "expenses", "total", "net"
]


def is_numeric(val):
    try:
        float(str(val).replace(",", ""))
        return True
    except:
        return False


def score_table(df: pd.DataFrame) -> float:
    if df.empty:
        return 0

    score = 0

    # Numeric density
    numeric_ratio = df.applymap(is_numeric).mean().mean()
    score += numeric_ratio * 2

    # Keyword presence
    text = " ".join(df.astype(str).values.flatten()).lower()
    keyword_hits = sum(kw in text for kw in FINANCIAL_KEYWORDS)
    score += keyword_hits * 1.5

    # Shape heuristics
    if 3 <= df.shape[1] <= 8:
        score += 1

    if df.shape[0] > 5:
        score += 1

    return score


USEFUL_THRESHOLD = 3.5


def filter_useful_tables(tables):
    scored = [(t, score_table(t)) for t in tables]
    return [t for t, s in scored if s >= USEFUL_THRESHOLD]


# =============================
# 3. NORMALISATION
# =============================

def normalize_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Drop empty rows/columns
    df = df.dropna(how="all").dropna(axis=1, how="all")

    if df.empty:
        return df

    # Reset column names
    df.columns = [f"col_{i}" for i in range(len(df.columns))]

    # Detect description column (most text-heavy)
    text_density = df.apply(lambda col: col.astype(str).str.len().mean())
    desc_col = text_density.idxmax()

    df = df.rename(columns={desc_col: "description"})

    # Clean numeric columns
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace(",", "")
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass

    return df


# =============================
# 4. MAIN PIPELINE
# =============================

def process_pdf(pdf_path):
    # Step 1: Extract
    tables = extract_all_tables(pdf_path)

    # Step 2: Filter
    useful_tables = filter_useful_tables(tables)

    # Step 3: Normalize
    normalized_tables = [normalize_table(t) for t in useful_tables]

    return normalized_tables


# =============================
# 5. BATCH PROCESSING
# =============================

def process_directory(pdf_paths):
    all_results = []

    for path in pdf_paths:
        try:
            tables = process_pdf(path)
            all_results.extend(tables)
        except Exception as e:
            print(f"Failed: {path} -> {e}")

    return all_results


# =============================
# 6. DATABASE STORAGE (NEON)
# =============================

def store_tables(engine, tables):
    for df in tables:
        if not df.empty:
            df.to_sql(
                "raw_financial_tables",
                engine,
                if_exists="append",
                index=False
            )


# =============================
# 7. USAGE EXAMPLE
# =============================

if __name__ == "__main__":
    pdf_files = [
        "snb.pdf",
        "aramco.pdf",
    ]

    results = process_directory(pdf_files)

    print(f"Extracted {len(results)} useful tables")

    # Example DB usage:
    # from sqlalchemy import create_engine
    # engine = create_engine("your_neon_connection_string")
    # store_tables(engine, results)
