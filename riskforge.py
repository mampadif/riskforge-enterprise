import streamlit as st
import google.generativeai as genai
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import stripe
import json
import re
import io
import base64
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List, Tuple
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.units import inch
from rapidfuzz import fuzz, process

# Optional imports with graceful fallback
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    EMBEDDING_AVAILABLE = True
except ImportError:
    EMBEDDING_AVAILABLE = False

# =============================================================================
# TAXONOMY FILTER - Block non-risk rows (Exploit, Enhance, Share, Accept, etc.)
# =============================================================================
NON_RISK_PATTERNS = [
    "opportunity response",
    "treatment strategy",
    "risk response",
    "exploit",
    "enhance",
    "share",
    "accept",
    "avoid",
    "transfer",
    "mitigate",
]

INVALID_RISK_PATTERNS = [
    "opportunity response",
    "risk monitoring",
    "risk register",
    "risk category",
    "treatment strategy",
    "response strategy",
    "quarter",
    "worksheet",
    "header",
    "instruction"
]

def looks_like_taxonomy_row(text: str) -> bool:
    """Identify ISO 31000 / COSO treatment strategy rows that are NOT risks."""
    if not text or not isinstance(text, str):
        return True
    t = text.lower().strip()
    for pattern in NON_RISK_PATTERNS:
        if t.startswith(pattern) or pattern in t.split()[:3]:
            return True
    words = t.split()
    if len(words) < 8:
        for word in words[:3]:
            if word in NON_RISK_PATTERNS:
                return True
    return False

def is_invalid_risk_header(text: str) -> bool:
    """Check if a risk name/statement is actually a structural header - safer version."""
    if not text or not isinstance(text, str):
        return True
    t = text.lower().strip()
    exact_headers = [
        "opportunity response",
        "risk monitoring",
        "risk register",
        "risk category",
        "treatment strategy",
        "response strategy",
        "worksheet",
        "header",
        "instruction",
        "approved by",
        "reviewed by",
        "signed off",
        "confidential",
        "footer",
        "signature",
    ]
    if t in exact_headers:
        return True
    if len(t.split()) <= 4:
        for h in exact_headers:
            if h in t:
                return True
    return False

# =============================================================================
# RELAXED RISK STATEMENT VALIDATOR (with optional Gemini fallback)
# =============================================================================
def gemini_is_risk_statement(text: str) -> bool:
    """Use Gemini to classify whether a statement is a genuine risk."""
    if not GEMINI_AVAILABLE or st.session_state.tier == "free":
        return False
    try:
        prompt = f"""
Determine whether the following sentence is a **risk statement** in an enterprise risk register.
A risk statement describes something that could go wrong, cause loss, delay, injury, non‑compliance, or harm to objectives.

Return JSON only: {{"is_risk": true}} or {{"is_risk": false}}

Sentence: {text[:300]}
"""
        response = ai_model.generate_content(prompt)
        result = response.text.strip()
        if result.startswith("```json"):
            result = result[7:]
        if result.endswith("```"):
            result = result[:-3]
        data = json.loads(result)
        return data.get("is_risk", False)
    except Exception:
        return False

def is_valid_risk_statement(text: str) -> bool:
    """Determine if a row represents a genuine risk, not metadata or strategy.
       Uses rule‑based filtering first, then Gemini for ambiguous cases."""
    if not isinstance(text, str):
        return False

    text_clean = text.strip()
    if len(text_clean) < 12:
        return False

    if looks_like_taxonomy_row(text_clean):
        return False

    if is_invalid_risk_header(text_clean):
        return False

    text_lower = text_clean.lower()

    # Block obvious non-risk phrases
    blocked_patterns = [
        "approved by", "reviewed by", "submitted by", "signed off",
        "page ", "confidential", "header", "footer", "signature",
        "current controls", "action plan", "treatment plan",
        "opportunity response", "treatment strategy"
    ]
    if any(pattern in text_lower for pattern in blocked_patterns):
        return False

    # Reject very short labels / headings
    if len(text_clean.split()) <= 2:
        return False

    # Generic risk indicators (no hardcoded department terms)
    risk_indicators = [
        "risk", "may", "might", "could", "potential", "failure", "loss", "delay", "disruption",
        "breach", "shortage", "inability", "lack of", "inadequate", "insufficient", "error",
        "failure to", "unable to", "problem", "issue", "concern", "threat", "vulnerability"
    ]
    if any(ind in text_lower for ind in risk_indicators):
        return True

    # Accept any reasonably long sentence (>=5 words) as a risk statement
    if len(text_clean.split()) >= 5:
        if GEMINI_AVAILABLE and st.session_state.tier != "free" and len(text_clean) > 30:
            return gemini_is_risk_statement(text_clean)
        return True

    return False

# =============================================================================
# REGISTER TITLE DETECTION
# =============================================================================
def detect_register_title(df: pd.DataFrame) -> Optional[str]:
    if df.empty:
        return None
    for col in df.columns:
        sample = df[col].astype(str).head(10)
        for val in sample:
            val_str = str(val).strip()
            if "risk monitoring" in val_str.lower():
                return val_str
            if "risk register" in val_str.lower() and len(val_str) < 60:
                return val_str
            if "quarter" in val_str.lower() and "risk" in val_str.lower():
                return val_str
    return None

# =============================================================================
# DIVISION DETECTION WITH CONFIDENCE
# =============================================================================
def detect_division_with_confidence(df: pd.DataFrame, file_name: str, row: pd.Series = None) -> Tuple[str, float, str]:
    confidence = 0.0
    source = "fallback"
    division = "Unassigned"
    
    if row is not None:
        for col in df.columns:
            col_lower = normalize_text(col)
            if any(term in col_lower for term in ["division", "department", "directorate", "unit", "business unit"]):
                val = str(row[col]) if pd.notna(row[col]) else ""
                if val and val.lower() not in ["not assigned", "unknown", "n/a", ""]:
                    division = val
                    confidence = 0.95
                    source = "dedicated column"
                    return division, confidence, source
    
    cleaned = clean_division_name(file_name)
    if cleaned and cleaned.lower() not in ["register", "risk register", "risk"]:
        division = cleaned
        confidence = 0.70
        source = "filename"
        return division, confidence, source
    
    return division, confidence, source

# =============================================================================
# CONFIGURATION & SECRETS
# =============================================================================
st.set_page_config(page_title="RiskForge Enterprise", page_icon="🛡️", layout="wide")

GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY")
STRIPE_SECRET_KEY = st.secrets.get("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID_PRO_MONTHLY = st.secrets.get("FORGE_STRIPE_PRICE_ID_PRO_MONTHLY")
STRIPE_PRICE_ID_PRO_ANNUAL = st.secrets.get("FORGE_STRIPE_PRICE_ID_PRO_ANNUAL")
STRIPE_PRICE_ID_ENT_MONTHLY = st.secrets.get("FORGE_STRIPE_PRICE_ID_ENT_MONTHLY")
STRIPE_PRICE_ID_ENT_ANNUAL = st.secrets.get("FORGE_STRIPE_PRICE_ID_ENT_ANNUAL")
APP_URL = st.secrets.get("APP_URL", "https://your-app.streamlit.app")
PRO_UNLOCK_CODE = st.secrets.get("PRO_UNLOCK_CODE", "PRO2025")
ENT_UNLOCK_CODE = st.secrets.get("ENT_UNLOCK_CODE", "ENT2025")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    GEMINI_AVAILABLE = True
    ai_model = genai.GenerativeModel("gemini-2.0-flash")
else:
    GEMINI_AVAILABLE = False
    ai_model = None

stripe.api_key = STRIPE_SECRET_KEY if STRIPE_SECRET_KEY else None

# =============================================================================
# SESSION STATE
# =============================================================================
if "tier" not in st.session_state:
    st.session_state.tier = "free"
if "rf_data" not in st.session_state:
    st.session_state.rf_data = None
if "history" not in st.session_state:
    st.session_state.history = []
if "org_name" not in st.session_state:
    st.session_state.org_name = "Your Organisation"
if "report_title" not in st.session_state:
    st.session_state.report_title = "Enterprise Risk Overview"
if "logo_bytes" not in st.session_state:
    st.session_state.logo_bytes = None
if "primary_color" not in st.session_state:
    st.session_state.primary_color = "#0E365C"
if "secondary_color" not in st.session_state:
    st.session_state.secondary_color = "#1A5F7A"
if "board_threshold" not in st.session_state:
    st.session_state.board_threshold = 12
if "default_residual_score" not in st.session_state:
    st.session_state.default_residual_score = 12
if "category_appetite" not in st.session_state:
    st.session_state.category_appetite = {}
if "parser_audit" not in st.session_state:
    st.session_state.parser_audit = None
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False

def handle_payment_success(plan: str):
    if plan in ("pro_monthly", "pro_annual"):
        st.session_state.tier = "professional"
    elif plan in ("ent_monthly", "ent_annual"):
        st.session_state.tier = "enterprise"

for param in ["success_pro_monthly", "success_pro_annual", "success_ent_monthly", "success_ent_annual"]:
    if param in st.query_params:
        if "pro" in param:
            handle_payment_success("pro_monthly" if "monthly" in param else "pro_annual")
        else:
            handle_payment_success("ent_monthly" if "monthly" in param else "ent_annual")
        st.query_params.clear()

# =============================================================================
# CACHING HELPERS
# =============================================================================
@st.cache_resource
def get_embedding_model():
    if EMBEDDING_AVAILABLE:
        return SentenceTransformer('all-MiniLM-L6-v2')
    return None

def make_json_serializable(obj: Any) -> Any:
    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [make_json_serializable(i) for i in obj]
    return obj

@st.cache_data(ttl=3600)
def cached_parse_file(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    return parse_uploaded_file_bytes(file_bytes, file_name, default_residual)

@st.cache_data(ttl=3600)
def cached_ai_summary(snapshot_json: str, company: str) -> str:
    snapshot = json.loads(snapshot_json)
    return ai_polish_narrative(snapshot, company)

# =============================================================================
# PARSER FUNCTIONS
# =============================================================================
def normalize_text(text: Any) -> str:
    if pd.isna(text):
        return ""
    return str(text).strip().lower()

def detect_header_row(df: pd.DataFrame, max_rows: int = 10) -> Tuple[int, float]:
    best_row = 0
    best_score = 0.0
    for i in range(min(max_rows, len(df))):
        row_vals = [normalize_text(x) for x in df.iloc[i].fillna("").tolist()]
        score = 0
        risk_keywords = ["risk", "statement", "description", "owner", "score", "category", "division", "impact", "likelihood"]
        for keyword in risk_keywords:
            if any(keyword in val for val in row_vals):
                score += 2
        if score > best_score:
            best_score = score
            best_row = i
    return best_row, best_score

def make_unique_columns(columns) -> list[str]:
    seen: dict[str, int] = {}
    unique_cols: list[str] = []
    for i, col in enumerate(columns):
        base = str(col).strip() if col is not None else ""
        if not base or base.lower() == "nan":
            base = f"col_{i}"
        count = seen.get(base, 0) + 1
        seen[base] = count
        if count == 1:
            unique_cols.append(base)
        else:
            unique_cols.append(f"{base}__{count}")
    return unique_cols

def parse_risk_score(val: Any) -> Optional[float]:
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    text_map = {
        "low": 5.0, "medium": 10.0, "moderate": 10.0, "high": 15.0,
        "very high": 20.0, "critical": 25.0, "extreme": 25.0,
        "minor": 5.0, "major": 15.0, "severe": 20.0,
    }
    if s in text_map:
        return text_map[s]
    match = re.search(r'(\d+(?:\.\d+)?)', s)
    if match:
        num = float(match.group(1))
        if 1 <= num <= 5:
            return num * 5
        if 1 <= num <= 10:
            return num * 2.5
        if 1 <= num <= 25:
            return num
    return None

def parse_excel_file_bytes(file_bytes: bytes, file_name: str) -> Tuple[pd.DataFrame, Dict]:
    all_sheets_data = []
    debug_info = {"sheets_processed": 0, "rows_scanned": 0, "sheets": []}
    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        for sheet_name in xls.sheet_names:
            sheet_info = {"sheet_name": sheet_name, "rows_scanned": 0, "rows_extracted": 0}
            df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None)
            if df_raw.empty or df_raw.shape[1] < 2:
                continue
            header_row, _ = detect_header_row(df_raw)
            sheet_info["header_row_detected"] = header_row
            if header_row > 0:
                raw_headers = [str(x).strip() if not pd.isna(x) else f"col_{i}" for i, x in enumerate(df_raw.iloc[header_row].tolist())]
                headers = make_unique_columns(raw_headers)
                df = df_raw.iloc[header_row + 1:].reset_index(drop=True)
                df.columns = headers
            else:
                df = df_raw.copy()
                df.columns = make_unique_columns([f"col_{i}" for i in range(df.shape[1])])
            df = df.dropna(how="all").reset_index(drop=True)
            if df.empty or len(df) < 2:
                continue
            if not df.columns.is_unique:
                df.columns = make_unique_columns(df.columns)
            sheet_info["rows_scanned"] = len(df)
            all_sheets_data.append(df)
            sheet_info["rows_extracted"] = len(df)
            debug_info["sheets_processed"] += 1
            debug_info["rows_scanned"] += len(df)
            debug_info["sheets"].append(sheet_info)
    except Exception as e:
        st.warning(f"Error parsing {file_name}: {e}")
        return pd.DataFrame(), debug_info
    if not all_sheets_data:
        return pd.DataFrame(), debug_info
    try:
        combined = pd.concat(all_sheets_data, ignore_index=True, sort=False)
        debug_info["total_rows_combined"] = len(combined)
    except Exception as e:
        st.error(f"Sheet consolidation failed for {file_name}: {e}")
        return pd.DataFrame(), debug_info
    return combined, debug_info

# -----------------------------------------------------------------------------
# INTELLIGENT COLUMN DETECTION (Fuzzy Matching)
# -----------------------------------------------------------------------------
def find_best_column(df: pd.DataFrame, target_keywords: List[str], threshold: int = 60) -> Optional[str]:
    if df.empty:
        return None
    col_names = [str(c).strip() for c in df.columns]
    best_match = None
    best_score = 0
    for col in col_names:
        for kw in target_keywords:
            score = fuzz.partial_ratio(col.lower(), kw.lower())
            if score > best_score:
                best_score = score
                best_match = col
    if best_score >= threshold:
        return best_match
    return None

def detect_risk_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    field_keywords = {
        "risk_statement": ["risk statement", "risk description", "description", "statement", "risk", "risk details", "risk event"],
        "risk_name": ["risk name", "risk title", "name", "title"],
        "residual_score": ["residual score", "residual risk", "current score", "score", "rating", "risk level", "residual rating"],
        "owner": ["risk owner", "owner", "responsible", "responsible party", "accountable"],
        "category": ["risk category", "category", "type", "classification"],
        "impact": ["impact", "impact rating", "severity", "consequence"],
        "likelihood": ["likelihood", "likelihood rating", "probability", "frequency"],
        "division": ["division", "department", "directorate", "unit", "business unit", "section"],
        "status": ["status", "risk status", "current status", "state"],
        "due_date": ["due date", "target date", "completion date", "deadline", "action date"],
        "control_effectiveness": ["control effectiveness", "effectiveness", "control rating", "control strength"]
    }
    mapping = {}
    for field, keywords in field_keywords.items():
        mapping[field] = find_best_column(df, keywords, threshold=55)
    return mapping

# -----------------------------------------------------------------------------
# TABULAR PARSER (row‑per‑risk)
# -----------------------------------------------------------------------------
def extract_risk_from_dataframe(df: pd.DataFrame, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    if df.empty:
        return pd.DataFrame(), {}
    
    detected_title = detect_register_title(df)
    if detected_title:
        st.session_state.report_title = detected_title
    
    # Use fuzzy column detection
    mapping = detect_risk_columns(df)
    risk_col = mapping.get("risk_statement")
    if risk_col is None:
        # Fallback: use first column with any text longer than 15 chars
        for col in df.columns:
            sample = df[col].astype(str).dropna()
            if len(sample) > 0 and any(len(str(x)) > 15 for x in sample):
                risk_col = col
                break
    if risk_col is None:
        risk_col = df.columns[0] if len(df.columns) > 0 else None
    if risk_col is None:
        return pd.DataFrame(), {"error": "No risk column found"}
    
    # Update mapping with the found risk column
    mapping["risk_statement"] = risk_col
    
    debug_info = {
        "mapping": {k: str(v) for k, v in mapping.items() if v},
        "rows_processed": 0,
        "rows_valid": 0,
        "rows_filtered_short": 0,
        "rows_filtered_taxonomy": 0,
    }

    risks = []
    seen_statements = set()
    
    for _, row in df.iterrows():
        debug_info["rows_processed"] += 1
        risk_text = str(row[risk_col]) if pd.notna(row[risk_col]) else ""
        if len(risk_text) < 12 or risk_text.startswith("=") or risk_text == "nan":
            debug_info["rows_filtered_short"] += 1
            continue
        
        if not is_valid_risk_statement(risk_text):
            debug_info["rows_filtered_taxonomy"] += 1
            continue
        
        if risk_text in seen_statements:
            continue
        seen_statements.add(risk_text)
        
        # Extract fields using mapping
        risk_name = str(row[mapping.get("risk_name")])[:80] if mapping.get("risk_name") and pd.notna(row[mapping["risk_name"]]) else risk_text[:50]
        
        residual_score = default_residual
        if mapping.get("residual_score") and pd.notna(row[mapping["residual_score"]]):
            residual_score = parse_risk_score(row[mapping["residual_score"]]) or default_residual
        impact_score = None
        if mapping.get("impact") and pd.notna(row[mapping["impact"]]):
            impact_score = parse_risk_score(row[mapping["impact"]])
        likelihood_score = None
        if mapping.get("likelihood") and pd.notna(row[mapping["likelihood"]]):
            likelihood_score = parse_risk_score(row[mapping["likelihood"]])
        if residual_score == default_residual and impact_score and likelihood_score:
            residual_score = min(25, max(1, (impact_score * likelihood_score) / 5))
        
        owner = str(row[mapping.get("owner")]) if mapping.get("owner") and pd.notna(row[mapping["owner"]]) else "Not assigned"
        
        category = str(row[mapping.get("category")]) if mapping.get("category") and pd.notna(row[mapping["category"]]) else None
        if not category or category == "nan":
            category = ai_infer_category(risk_text) if GEMINI_AVAILABLE else "Uncategorised"
        
        division, div_confidence, div_source = detect_division_with_confidence(df, file_name, row)
        if mapping.get("division") and pd.notna(row[mapping["division"]]):
            division = str(row[mapping["division"]])
            div_confidence = 0.9
            div_source = "detected_column"
        
        status = str(row[mapping.get("status")]) if mapping.get("status") and pd.notna(row[mapping["status"]]) else "Not specified"
        due_date_raw = row[mapping.get("due_date")] if mapping.get("due_date") and pd.notna(row[mapping["due_date"]]) else None
        due_date = None
        if due_date_raw:
            try:
                due_date = pd.to_datetime(due_date_raw).date()
            except:
                pass
        control_effectiveness = str(row[mapping.get("control_effectiveness")]) if mapping.get("control_effectiveness") and pd.notna(row[mapping["control_effectiveness"]]) else "Not rated"
        
        risks.append({
            "division": division,
            "division_confidence": div_confidence,
            "division_source": div_source,
            "risk_name": risk_name,
            "risk_statement": risk_text[:500],
            "category": category,
            "residual_score": min(25, max(1, residual_score)),
            "inherent_score": min(25, residual_score + 3),
            "owner": owner,
            "status": status,
            "due_date": due_date,
            "control_effectiveness": control_effectiveness,
            "impact_score": impact_score,
            "likelihood_score": likelihood_score,
        })
        debug_info["rows_valid"] += 1
    
    return pd.DataFrame(risks), debug_info

def assess_parser_confidence(mapping: Dict[str, str], has_impact_likelihood: bool) -> Tuple[float, str, List[str]]:
    required_risk = ["risk_statement"]
    required_score = ["residual_score"]
    found_risk = sum(1 for f in required_risk if f in mapping)
    found_score = sum(1 for f in required_score if f in mapping)
    base = (found_risk / len(required_risk)) * 50
    if found_score > 0:
        score = base + 50
    elif has_impact_likelihood:
        score = base + 40
    else:
        score = base + 10
    score = min(100, max(0, score))
    level = "High" if score >= 80 else "Medium" if score >= 50 else "Low"
    inferred = [f"{k} -> {v}" for k, v in mapping.items() if v]
    return score, level, inferred

# -----------------------------------------------------------------------------
# FORM-BASED PARSER (one risk per sheet)
# -----------------------------------------------------------------------------
def is_form_sheet(df: pd.DataFrame) -> bool:
    if df.empty or df.shape[1] < 2:
        return False
    text = " ".join(df.astype(str).values.flatten()).lower()
    labels = ["name of risk", "risk name", "risk definition", "risk statement", "risk owner", "impact rating", "likelihood rating"]
    return any(label in text for label in labels)

def extract_form_risk(df: pd.DataFrame, sheet_name: str, default_residual: int) -> Optional[Dict]:
    cells = []
    for i in range(min(200, df.shape[0])):
        for j in range(min(50, df.shape[1])):
            val = df.iat[i, j]
            if pd.notna(val):
                cells.append((i, j, str(val).strip()))
    label_map = {
        "risk_name": ["name of risk", "risk name"],
        "risk_statement": ["risk definition", "risk statement", "definition"],
        "owner": ["risk owner", "owner"],
        "impact_rating": ["impact rating", "impact"],
        "likelihood_rating": ["likelihood rating", "likelihood"],
        "residual_risk": ["residual risk", "residual"],
        "division": ["department/ unit", "division", "unit"]
    }
    extracted = {}
    for field, patterns in label_map.items():
        for i, j, label in cells:
            label_lower = label.lower()
            if any(p in label_lower for p in patterns):
                value = None
                for offset in range(1, 11):
                    if j + offset < df.shape[1]:
                        val = df.iat[i, j+offset]
                        if pd.notna(val) and str(val).strip():
                            value = str(val).strip()
                            break
                if not value:
                    for offset in range(1, 6):
                        if i+offset < df.shape[0]:
                            val = df.iat[i+offset, j]
                            if pd.notna(val) and str(val).strip():
                                value = str(val).strip()
                                break
                if value:
                    extracted[field] = value
                    break
    risk_name = extracted.get("risk_name", "")
    risk_statement = extracted.get("risk_statement", "")
    if not risk_statement and risk_name:
        risk_statement = risk_name
    owner = extracted.get("owner", "Not assigned")
    division = extracted.get("division", sheet_name)
    impact_score = None
    if "impact_rating" in extracted:
        m = re.search(r'(\d+)', extracted["impact_rating"])
        if m:
            impact_score = int(m.group(1))
    likelihood_score = None
    if "likelihood_rating" in extracted:
        m = re.search(r'(\d+)', extracted["likelihood_rating"])
        if m:
            likelihood_score = int(m.group(1))
    residual_score = default_residual
    if "residual_risk" in extracted:
        m = re.search(r'(\d+)', extracted["residual_risk"])
        if m:
            residual_score = int(m.group(1))
    elif impact_score and likelihood_score:
        residual_score = impact_score * likelihood_score
        if residual_score > 25:
            residual_score = min(25, residual_score // 5)
    if not risk_statement or len(risk_statement) < 15:
        return None
    return {
        "division": division,
        "division_confidence": 0.8,
        "division_source": "form_sheet",
        "risk_name": risk_name[:80] if risk_name else risk_statement[:50],
        "risk_statement": risk_statement[:500],
        "category": "Uncategorised",
        "residual_score": min(25, max(1, residual_score)),
        "inherent_score": min(25, residual_score + 3),
        "owner": owner,
        "status": "Active",
        "due_date": None,
        "control_effectiveness": "Not rated",
        "impact_score": impact_score,
        "likelihood_score": likelihood_score,
    }

def parse_form_sheets(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    all_risks = []
    debug = {"sheets_processed": 0, "risks_extracted": 0}
    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        for sheet in xls.sheet_names:
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)
            if is_form_sheet(df):
                debug["sheets_processed"] += 1
                risk = extract_form_risk(df, sheet, default_residual)
                if risk:
                    all_risks.append(risk)
                    debug["risks_extracted"] += 1
    except:
        pass
    if all_risks:
        return pd.DataFrame(all_risks), debug
    return pd.DataFrame(), debug

# -----------------------------------------------------------------------------
# LAST RESORT PARSER (first non-empty column as risk statement)
# -----------------------------------------------------------------------------
def last_resort_parser(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    try:
        if file_name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_bytes), header=None)
            all_text = df[0].dropna().astype(str).tolist()
        else:
            xls = pd.ExcelFile(io.BytesIO(file_bytes))
            all_text = []
            for sheet in xls.sheet_names:
                sheet_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)
                for col in sheet_df.columns:
                    vals = sheet_df[col].dropna().astype(str).tolist()
                    if vals:
                        all_text.extend(vals)
                        break
    except Exception as e:
        return pd.DataFrame(), {"error": f"Last resort failed: {e}"}
    risks = []
    for text in all_text:
        text = str(text).strip()
        if len(text) < 12 or looks_like_taxonomy_row(text) or is_invalid_risk_header(text):
            continue
        risks.append({
            "division": clean_division_name(file_name),
            "division_confidence": 0.5,
            "division_source": "last_resort",
            "risk_name": text[:50],
            "risk_statement": text[:500],
            "category": "Uncategorised",
            "residual_score": default_residual,
            "inherent_score": min(25, default_residual + 3),
            "owner": "Not assigned",
            "status": "Active",
            "due_date": None,
            "control_effectiveness": "Not rated",
            "impact_score": None,
            "likelihood_score": None,
        })
    if risks:
        return pd.DataFrame(risks), {"last_resort_extracted": len(risks)}
    return pd.DataFrame(), {"error": "No risks found"}

# -----------------------------------------------------------------------------
# GEMINI FALLBACK PARSER (Professional/Enterprise only)
# -----------------------------------------------------------------------------
def ai_parse_file(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    if not GEMINI_AVAILABLE or st.session_state.tier == "free":
        return pd.DataFrame(), {"error": "Gemini not available"}
    try:
        if file_name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_bytes))
        else:
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
            combined = pd.concat(df.values(), ignore_index=True)
        content = combined.astype(str).to_string()
    except:
        content = "Could not parse file content"
    prompt = f"""
You are a risk extraction engine. Extract all risks as a JSON list.
Each risk must have: risk_statement (string), risk_name (optional), owner (string), residual_score (1-25), category (string from: Strategic, Financial, Operational, ICT/Cyber, Compliance/Legal, People/HR, Health/Safety, Reputational, Environmental), division (optional), impact_score (1-5 optional), likelihood_score (1-5 optional).
Return ONLY valid JSON. No explanation.
Content: {content[:6000]}
"""
    try:
        response = ai_model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.endswith("```"):
            text = text[:-3]
        risks = json.loads(text)
        if isinstance(risks, dict) and "risks" in risks:
            risks = risks["risks"]
        if not isinstance(risks, list):
            risks = [risks]
        rows = []
        for r in risks:
            rows.append({
                "division": r.get("division", "Unassigned"),
                "division_confidence": 0.5,
                "division_source": "ai_fallback",
                "risk_name": r.get("risk_name", r.get("risk_statement", "")[:50]),
                "risk_statement": r.get("risk_statement", ""),
                "category": r.get("category", "Uncategorised"),
                "residual_score": min(25, max(1, int(r.get("residual_score", default_residual)))),
                "inherent_score": min(25, int(r.get("residual_score", default_residual)) + 3),
                "owner": r.get("owner", "Not assigned"),
                "status": "Active",
                "due_date": None,
                "control_effectiveness": "Not rated",
                "impact_score": r.get("impact_score"),
                "likelihood_score": r.get("likelihood_score"),
            })
        return pd.DataFrame(rows), {"ai_extracted": len(rows)}
    except:
        return pd.DataFrame(), {"error": "AI extraction failed"}

# -----------------------------------------------------------------------------
# MAIN PARSER DISPATCHER
# -----------------------------------------------------------------------------
def parse_uploaded_file_bytes(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    # Strategy 1: Tabular with intelligent column detection
    try:
        if file_name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_bytes))
        else:
            df, _ = parse_excel_file_bytes(file_bytes, file_name)
        if not df.empty:
            risks_df, debug = extract_risk_from_dataframe(df, file_name, default_residual)
            if not risks_df.empty:
                return risks_df, debug
    except:
        pass
    
    # Strategy 2: Form-based (one risk per sheet)
    risks_df, debug_form = parse_form_sheets(file_bytes, file_name, default_residual)
    if not risks_df.empty:
        return risks_df, debug_form
    
    # Strategy 3: Gemini fallback (Professional/Enterprise only)
    if st.session_state.tier != "free" and GEMINI_AVAILABLE:
        risks_df, debug_ai = ai_parse_file(file_bytes, file_name, default_residual)
        if not risks_df.empty:
            return risks_df, debug_ai
    
    # Strategy 4: Last resort
    risks_df, debug_last = last_resort_parser(file_bytes, file_name, default_residual)
    if not risks_df.empty:
        return risks_df, debug_last
    
    return pd.DataFrame(), {"error": "No risks could be extracted"}

def clean_division_name(filename: str) -> str:
    name = re.sub(r"\.xlsx$|\.xls$|\.csv$", "", filename, flags=re.IGNORECASE)
    name = re.sub(r"^copy of\s+", "", name, flags=re.IGNORECASE)
    name = name.replace("_", " ").strip()
    name = re.sub(r"\s+", " ", name)
    return name.title() if name else "Unknown Division"

def parse_all_files(uploaded_files, tier: str, default_residual: int) -> Tuple[pd.DataFrame, List[Dict]]:
    all_risks = []
    all_debug = []
    total_rows_scanned = 0
    total_rows_extracted = 0
    total_filtered_taxonomy = 0
    total_filtered_header = 0
    total_filtered_short = 0
    
    for file in uploaded_files:
        df, debug = cached_parse_file(file.getvalue(), file.name, default_residual)
        all_debug.append(debug)
        if not df.empty:
            all_risks.append(df)
            total_rows_extracted += len(df)
            total_filtered_taxonomy += debug.get("rows_filtered_taxonomy", 0)
            total_filtered_header += debug.get("rows_filtered_header", 0)
            total_filtered_short += debug.get("rows_filtered_short", 0)
    
    if not all_risks:
        return pd.DataFrame(), all_debug
    
    df_all = pd.concat(all_risks, ignore_index=True)
    df_all, _ = detect_semantic_duplicates(df_all, threshold=0.85)
    unique_risk_count = df_all["risk_statement"].nunique()
    df_all["residual_level"] = df_all["residual_score"].apply(lambda x: "Critical" if x >= 20 else "High" if x >= 12 else "Medium" if x >= 6 else "Low")
    if tier == "free":
        df_all = df_all.head(10)
    
    audit_summary = {
        "total_files": len(uploaded_files),
        "total_rows_scanned": sum(d.get("rows_scanned", 0) for d in all_debug),
        "total_rows_extracted": total_rows_extracted,
        "total_filtered_taxonomy": total_filtered_taxonomy,
        "total_filtered_header": total_filtered_header,
        "total_filtered_short": total_filtered_short,
        "unique_risks": unique_risk_count,
        "division_confidence": "Mixed"
    }
    st.session_state.parser_audit = audit_summary
    return df_all, all_debug

# =============================================================================
# SEMANTIC DUPLICATE DETECTION
# =============================================================================
def normalize_for_dedupe(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def embedding_similarity(texts: List[str]) -> Optional[np.ndarray]:
    if not EMBEDDING_AVAILABLE or len(texts) < 2:
        return None
    model = get_embedding_model()
    if model is None:
        return None
    try:
        embeddings = model.encode(texts)
        return cosine_similarity(embeddings)
    except Exception:
        return None

def detect_semantic_duplicates(df: pd.DataFrame, threshold: float = 0.85) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, pd.DataFrame()
    df = df.copy()
    df["_normalized"] = df["risk_statement"].fillna("").apply(normalize_for_dedupe)
    df["_hash"] = df["_normalized"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    df = df.drop_duplicates(subset=["_hash"], keep="first")
    keep_indices = []
    duplicate_map = []
    statements = df["_normalized"].tolist()
    sim_matrix = embedding_similarity(statements) if EMBEDDING_AVAILABLE and len(statements) > 1 else None
    
    for i, stmt in enumerate(statements):
        is_duplicate = False
        for j in keep_indices:
            if j >= len(statements):
                continue
            if sim_matrix is not None and i < sim_matrix.shape[0] and j < sim_matrix.shape[1]:
                similarity = sim_matrix[i][j]
            else:
                similarity = fuzz.ratio(stmt, statements[j]) / 100.0
            if similarity >= threshold:
                duplicate_map.append({"kept_index": j, "dropped_index": i, "similarity": similarity})
                is_duplicate = True
                break
        if not is_duplicate:
            keep_indices.append(i)
    deduped = df.iloc[keep_indices].reset_index(drop=True)
    duplicates_df = pd.DataFrame(duplicate_map) if duplicate_map else pd.DataFrame()
    deduped = deduped.drop(columns=["_normalized", "_hash"])
    return deduped, duplicates_df

# =============================================================================
# AI FUNCTIONS (for category inference, narrative, etc.)
# =============================================================================
def ai_infer_category(statement: str, fallback: str = "Uncategorised") -> str:
    categories = ["Strategic", "Financial", "Operational", "ICT/Cyber", "Compliance/Legal", "People/HR", "Health/Safety", "Reputational", "Environmental"]
    if not GEMINI_AVAILABLE or len(statement) < 20:
        return fallback
    try:
        prompt = f"""Classify this risk into exactly one of these categories: {', '.join(categories)}.
Return ONLY the category name, nothing else.
Risk: {statement[:400]}"""
        response = ai_model.generate_content(prompt)
        result = response.text.strip()
        if result in categories:
            return result
    except:
        pass
    return fallback

def ai_polish_narrative(snapshot: Dict, company: str) -> str:
    if not GEMINI_AVAILABLE:
        return ""
    try:
        prompt = f"""Write a professional, board‑ready executive briefing (2‑3 sentences) for {company}:
- Health score: {snapshot.get('enterprise_health_score', 0)}/100
- Critical/high risks: {snapshot.get('critical_count', 0) + snapshot.get('high_count', 0)}
- Top division: {snapshot.get('top_division', 'N/A')}
- Appetite breached: {snapshot.get('pct_breached', 0)}%
Be concise, actionable, and use professional risk language."""
        response = ai_model.generate_content(prompt)
        return response.text.strip() if response.text else ""
    except:
        return ""

# =============================================================================
# WEIGHTED TREATMENT CONFIDENCE
# =============================================================================
def calculate_weighted_treatment_confidence(row: pd.Series) -> float:
    score = 0.0
    weights = {"owner": 0.3, "due_date": 0.25, "status": 0.25, "control": 0.2}
    if row.get("owner") and row["owner"].lower() != "not assigned":
        score += weights["owner"] * 100
    due = row.get("due_date")
    if due and isinstance(due, (date, pd.Timestamp)):
        if due >= date.today():
            score += weights["due_date"] * 100
        else:
            score += weights["due_date"] * 30
    elif due:
        score += weights["due_date"] * 50
    else:
        score += weights["due_date"] * 20
    status = str(row.get("status", "")).lower()
    if "completed" in status or "closed" in status:
        score += weights["status"] * 100
    elif "on track" in status or "active" in status or "in progress" in status:
        score += weights["status"] * 70
    elif "overdue" in status or "delayed" in status:
        score += weights["status"] * 30
    else:
        score += weights["status"] * 50
    eff = str(row.get("control_effectiveness", "")).lower()
    if "effective" in eff or "strong" in eff:
        score += weights["control"] * 100
    elif "partial" in eff or "moderate" in eff:
        score += weights["control"] * 60
    elif "weak" in eff or "ineffective" in eff:
        score += weights["control"] * 20
    else:
        score += weights["control"] * 40
    return round(score, 1)

# =============================================================================
# RISK INTELLIGENCE ENGINE
# =============================================================================
def appetite_band(score: float, threshold: int, category: str = "", category_appetite: Dict = None) -> str:
    if pd.isna(score):
        return "unknown"
    if category_appetite and category in category_appetite:
        threshold = category_appetite[category]
    if score >= threshold + 4:
        return "critical breach"
    if score >= threshold:
        return "breached"
    if score >= threshold - 4:
        return "near appetite"
    return "within appetite"

def calculate_enterprise_health_score(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    score = 100.0
    critical_pct = (df["residual_level"] == "Critical").mean() * 100
    score -= critical_pct * 0.8
    unassigned_pct = (df["owner"].astype(str).str.lower() == "not assigned").mean() * 100
    score -= unassigned_pct * 0.5
    high_pct = (df["residual_level"] == "High").mean() * 100
    score -= high_pct * 0.3
    return max(0.0, round(score, 1))

def detect_emerging_themes(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return []
    theme_keywords = {
        "Cyber & Data Security": ["cyber", "ransomware", "data breach", "hacking", "phishing", "malware"],
        "Supply Chain": ["supplier", "vendor", "third party", "outsource", "dependency"],
        "Regulatory Change": ["regulation", "compliance", "legislation", "law change"],
        "Talent & Workforce": ["staff", "skills", "turnover", "recruitment", "retention"],
        "Technology Disruption": ["digital", "automation", "ai", "artificial intelligence", "legacy system"],
        "Financial Pressure": ["budget", "funding", "capital", "investment", "cash flow", "liquidity"]
    }
    themes = []
    for theme, keywords in theme_keywords.items():
        count = sum(1 for stmt in df["risk_statement"].fillna("").astype(str) if any(kw in stmt.lower() for kw in keywords))
        if count >= 2 and (count / len(df)) >= 0.08:
            themes.append(theme)
    return themes

def build_intelligence_snapshot(df: pd.DataFrame, threshold: int, category_appetite: Dict = None) -> Dict[str, Any]:
    if df.empty:
        return {}
    snapshot = {}
    snapshot["critical_count"] = int((df["residual_level"] == "Critical").sum())
    snapshot["high_count"] = int((df["residual_level"] == "High").sum())
    snapshot["avg_residual"] = round(df["residual_score"].mean(), 1)
    snapshot["avg_inherent"] = round(df["inherent_score"].mean(), 1)
    
    exposure_by_div = df.groupby("division")["residual_score"].sum().sort_values(ascending=False)
    if not exposure_by_div.empty:
        snapshot["top_division"] = exposure_by_div.index[0]
        snapshot["top_division_pct"] = round((exposure_by_div.iloc[0] / exposure_by_div.sum()) * 100, 1)
        snapshot["division_exposure"] = exposure_by_div.head(5).to_dict()
    else:
        snapshot["top_division"] = "N/A"
        snapshot["top_division_pct"] = 0
        snapshot["division_exposure"] = {}
    
    exposure_by_cat = df.groupby("category")["residual_score"].sum().sort_values(ascending=False)
    snapshot["category_exposure"] = exposure_by_cat.head(5).to_dict()
    
    df["appetite_band"] = df.apply(lambda row: appetite_band(row["residual_score"], threshold, row.get("category", ""), category_appetite), axis=1)
    snapshot["pct_within_appetite"] = round((df["appetite_band"] == "within appetite").mean() * 100, 1)
    snapshot["pct_near_appetite"] = round((df["appetite_band"] == "near appetite").mean() * 100, 1)
    snapshot["pct_breached"] = round((df["appetite_band"].isin(["breached", "critical breach"])).mean() * 100, 1)
    
    snapshot["emerging_themes"] = detect_emerging_themes(df)
    snapshot["ownership_coverage"] = round((df["owner"].astype(str).str.lower() != "not assigned").mean() * 100, 1)
    snapshot["enterprise_health_score"] = calculate_enterprise_health_score(df)
    df["treatment_confidence"] = df.apply(calculate_weighted_treatment_confidence, axis=1)
    snapshot["treatment_confidence"] = round(df["treatment_confidence"].mean(), 1)
    snapshot["total_risks"] = len(df)
    snapshot["board_risks"] = df.nlargest(5, "residual_score")[["risk_name", "division", "residual_score", "owner", "category"]].to_dict("records")
    snapshot["risks_list"] = df[["risk_name", "residual_score", "owner", "division"]].to_dict("records")
    return snapshot

def compare_snapshots(current: Dict, previous: Dict) -> Dict[str, Any]:
    if not previous or not current:
        return {}
    current_risks = {r["risk_name"]: r for r in current.get("risks_list", [])}
    previous_risks = {r["risk_name"]: r for r in previous.get("risks_list", [])}
    return {
        "new_risks": [current_risks[n] for n in current_risks if n not in previous_risks],
        "closed_risks": [previous_risks[n] for n in previous_risks if n not in current_risks],
        "worsened_risks": [{"name": n, "delta": current_risks[n]["residual_score"] - previous_risks[n]["residual_score"]}
                           for n in current_risks if n in previous_risks and current_risks[n]["residual_score"] > previous_risks[n]["residual_score"] + 1],
        "improved_risks": [{"name": n, "delta": current_risks[n]["residual_score"] - previous_risks[n]["residual_score"]}
                           for n in current_risks if n in previous_risks and current_risks[n]["residual_score"] < previous_risks[n]["residual_score"] - 1],
        "health_delta": current.get("enterprise_health_score", 0) - previous.get("enterprise_health_score", 0),
        "appetite_delta": current.get("pct_breached", 0) - previous.get("pct_breached", 0),
    }

# =============================================================================
# BOARD NARRATIVE
# =============================================================================
def generate_board_narrative(snapshot: Dict, comparison: Dict, threshold: int, company: str, report_title: str, ai_summary: str = "") -> str:
    narrative = []
    narrative.append(f"# {report_title}")
    narrative.append(f"**Organization:** {company}")
    narrative.append(f"**Date:** {datetime.now().strftime('%B %d, %Y')}")
    narrative.append(f"**Reporting Period:** Q{((datetime.now().month-1)//3)+1} {datetime.now().year}")
    narrative.append("")
    if ai_summary:
        narrative.append(f"**AI Executive Summary:** {ai_summary}")
        narrative.append("")
    health = snapshot.get("enterprise_health_score", 0)
    posture = "Strong" if health >= 80 else "Stable" if health >= 60 else "Elevated" if health >= 40 else "Critical"
    narrative.append(f"## 1. Executive Posture Summary")
    narrative.append(f"**Enterprise Health Score:** {health}/100 ({posture})")
    narrative.append(f"**Total Risks:** {snapshot.get('critical_count', 0) + snapshot.get('high_count', 0)} critical/high, {snapshot.get('avg_residual', 0):.1f}/25 average residual")
    narrative.append("")
    if comparison:
        narrative.append(f"## 2. Movement Since Last Review")
        if comparison.get("new_risks"):
            narrative.append(f"- **New risks:** {len(comparison['new_risks'])}")
        if comparison.get("closed_risks"):
            narrative.append(f"- **Closed risks:** {len(comparison['closed_risks'])}")
        if comparison.get("worsened_risks"):
            narrative.append(f"- **Worsened risks:** {len(comparison['worsened_risks'])}")
        if comparison.get("improved_risks"):
            narrative.append(f"- **Improved risks:** {len(comparison['improved_risks'])}")
        narrative.append("")
    narrative.append(f"## 3. Concentration Risk Areas")
    narrative.append(f"**Top Division Exposure:** {snapshot.get('top_division', 'N/A')} ({snapshot.get('top_division_pct', 0)}% of enterprise load)")
    narrative.append("")
    narrative.append(f"## 4. Risk Appetite Status (Threshold: {threshold}/25)")
    narrative.append(f"- Within appetite: {snapshot.get('pct_within_appetite', 0)}%")
    narrative.append(f"- Near appetite: {snapshot.get('pct_near_appetite', 0)}%")
    narrative.append(f"- Breached: {snapshot.get('pct_breached', 0)}%")
    narrative.append("")
    narrative.append(f"## 5. Treatment Delivery Confidence")
    narrative.append(f"**Confidence Score:** {snapshot.get('treatment_confidence', 0)}%")
    narrative.append(f"**Ownership coverage:** {snapshot.get('ownership_coverage', 0)}%")
    narrative.append("")
    if snapshot.get("board_risks"):
        narrative.append(f"## 6. Top 5 Board-Attention Risks")
        for risk in snapshot["board_risks"]:
            narrative.append(f"- **{risk['risk_name']}** ({risk['division']}) – Residual: {risk['residual_score']}/25, Owner: {risk['owner']}")
        narrative.append("")
    themes = snapshot.get("emerging_themes", [])
    if themes:
        narrative.append(f"## 7. Emerging Systemic Themes")
        for theme in themes:
            narrative.append(f"- {theme}")
        narrative.append("")
    return "\n".join(narrative)

# =============================================================================
# EXCEL & PDF EXPORTS
# =============================================================================
def style_excel_with_risk_colors(wb):
    level_colors = {
        "Critical": PatternFill(start_color="FEE2E2", end_color="FEE2E2", fill_type="solid"),
        "High": PatternFill(start_color="FFEDD5", end_color="FFEDD5", fill_type="solid"),
        "Medium": PatternFill(start_color="FEF3C7", end_color="FEF3C7", fill_type="solid"),
        "Low": PatternFill(start_color="DCFCE7", end_color="DCFCE7", fill_type="solid")
    }
    for sheetname in wb.sheetnames:
        ws = wb[sheetname]
        header_row = 1
        level_col_idx = None
        for col_idx, cell in enumerate(ws[header_row], 1):
            if cell.value and "residual_level" in str(cell.value).lower():
                level_col_idx = col_idx
                break
        if level_col_idx:
            for row in range(2, ws.max_row + 1):
                level = ws.cell(row=row, column=level_col_idx).value
                fill = level_colors.get(level, PatternFill())
                for col in range(1, ws.max_column + 1):
                    ws.cell(row=row, column=col).fill = fill

def generate_excel_pack(data: Dict[str, Any], narrative: str) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary = pd.DataFrame({
            "Metric": ["Organization", "Report Title", "Period", "Board Date", "Health Score", "Total Risks", "Critical+High", "Avg Residual", "Top Division", "Treatment Confidence", "Within Appetite", "Near Appetite", "Breached"],
            "Value": [
                data.get("company", ""), data.get("report_title", ""), data.get("period", ""), data.get("board_date", ""),
                data.get("enterprise_health_score", 0), data["total_risks"],
                data.get("critical_count", 0) + data.get("high_count", 0),
                f"{data.get('avg_residual', 0):.1f}/25", f"{data.get('top_division', 'N/A')} ({data.get('top_division_pct', 0)}%)",
                f"{data.get('treatment_confidence', 0)}%", f"{data.get('pct_within_appetite', 0)}%",
                f"{data.get('pct_near_appetite', 0)}%", f"{data.get('pct_breached', 0)}%"
            ]
        })
        summary.to_excel(writer, sheet_name="Executive Summary", index=False)
        pd.DataFrame({"Board Narrative": narrative.split("\n")}).to_excel(writer, sheet_name="Board Narrative", index=False)
        data["risks_df"].to_excel(writer, sheet_name="Enterprise Risks", index=False)
        board_risks = data["risks_df"][data["risks_df"]["residual_score"] >= data.get("threshold", 12)].copy()
        if not board_risks.empty:
            board_risks.to_excel(writer, sheet_name="Board Attention", index=False)
        if data.get("division_exposure"):
            div_df = pd.DataFrame(list(data["division_exposure"].items()), columns=["Division", "Exposure"])
            div_df.to_excel(writer, sheet_name="Division Exposure", index=False)
        if data.get("category_exposure"):
            cat_df = pd.DataFrame(list(data["category_exposure"].items()), columns=["Category", "Exposure"])
            cat_df.to_excel(writer, sheet_name="Category Exposure", index=False)
        if st.session_state.parser_audit:
            audit_df = pd.DataFrame([st.session_state.parser_audit])
            audit_df.to_excel(writer, sheet_name="Parser Audit", index=False)
    output.seek(0)
    wb = load_workbook(output)
    style_excel_with_risk_colors(wb)
    styled_output = io.BytesIO()
    wb.save(styled_output)
    styled_output.seek(0)
    return styled_output

def generate_pdf_board_pack(narrative: str, snapshot: Dict, company: str, report_title: str, logo_bytes: bytes = None) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter))
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, alignment=TA_CENTER, spaceAfter=20, textColor=colors.HexColor("#0E365C"))
    heading_style = ParagraphStyle('Heading', parent=styles['Heading2'], fontSize=14, spaceBefore=15, spaceAfter=10, textColor=colors.HexColor("#1A5F7A"))
    normal_style = ParagraphStyle('Normal', parent=styles['Normal'], fontSize=10, spaceAfter=6)
    story = []
    if logo_bytes:
        img = Image(io.BytesIO(logo_bytes), width=1.5*inch, height=0.75*inch)
        story.append(img)
    story.append(Paragraph(f"{report_title}", title_style))
    story.append(Paragraph(f"{company}", title_style))
    story.append(Spacer(1, 12))
    health = snapshot.get("enterprise_health_score", 0)
    story.append(Paragraph(f"Enterprise Health Score: {health}/100", heading_style))
    story.append(Spacer(1, 6))
    for line in narrative.split("\n"):
        if line.startswith("#"):
            story.append(Paragraph(line[2:], heading_style))
        elif line.strip():
            story.append(Paragraph(line, normal_style))
        else:
            story.append(Spacer(1, 6))
    if snapshot.get("board_risks"):
        story.append(Spacer(1, 12))
        story.append(Paragraph("Top Board-Attention Risks (Color Coded)", heading_style))
        data = []
        for risk in snapshot["board_risks"][:5]:
            level = "Critical" if risk["residual_score"] >= 20 else "High" if risk["residual_score"] >= 12 else "Medium" if risk["residual_score"] >= 6 else "Low"
            data.append([risk["risk_name"], risk["division"], f"{risk['residual_score']}/25", risk["owner"], level])
        t = Table([["Risk Name", "Division", "Residual", "Owner", "Level"]] + data, colWidths=[200, 100, 60, 100, 60])
        style = [('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0E365C")), ('TEXTCOLOR', (0,0), (-1,0), colors.white)]
        for i, row in enumerate(data, start=1):
            level = row[4]
            if level == "Critical":
                bg = colors.HexColor("#FEE2E2")
            elif level == "High":
                bg = colors.HexColor("#FFEDD5")
            elif level == "Medium":
                bg = colors.HexColor("#FEF3C7")
            else:
                bg = colors.HexColor("#DCFCE7")
            style.append(('BACKGROUND', (0,i), (-1,i), bg))
        t.setStyle(style)
        story.append(t)
    if st.session_state.parser_audit:
        story.append(Spacer(1, 12))
        story.append(Paragraph("Parser Audit Summary", heading_style))
        audit_data = [[k, str(v)] for k, v in st.session_state.parser_audit.items()]
        audit_table = Table(audit_data, colWidths=[150, 150])
        audit_table.setStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0E365C")), ('TEXTCOLOR', (0,0), (-1,0), colors.white)])
        story.append(audit_table)
    doc.build(story)
    return buffer.getvalue()

# =============================================================================
# DASHBOARD CHARTS
# =============================================================================
def create_category_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    cat_exposure = df.groupby("category")["residual_score"].sum().sort_values(ascending=False).head(8)
    fig = go.Figure(data=[go.Bar(x=list(cat_exposure.values), y=list(cat_exposure.index), orientation='h', marker_color='#4A90E2')])
    fig.update_layout(title="Risk Exposure by Category", height=350, plot_bgcolor="white", margin=dict(l=10, r=10, t=40, b=10))
    return fig

def create_division_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    div_exposure = df.groupby("division")["residual_score"].sum().sort_values(ascending=False).head(8)
    fig = go.Figure(data=[go.Bar(x=list(div_exposure.index), y=list(div_exposure.values), marker_color='#F97316')])
    fig.update_layout(title="Risk Exposure by Division", height=350, xaxis_tickangle=-30, plot_bgcolor="white", margin=dict(l=10, r=10, t=40, b=80))
    return fig

def create_appetite_gauge(df: pd.DataFrame, threshold: int, category_appetite: Dict = None) -> go.Figure:
    if df.empty:
        return go.Figure()
    df["appetite"] = df.apply(lambda row: appetite_band(row["residual_score"], threshold, row.get("category", ""), category_appetite), axis=1)
    counts = df["appetite"].value_counts()
    labels = ["Within", "Near", "Breached"]
    values = [counts.get("within appetite", 0), counts.get("near appetite", 0), counts.get("breached", 0) + counts.get("critical breach", 0)]
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4, marker_colors=["#10B981", "#F59E0B", "#EF4444"])])
    fig.update_layout(title="Risk Appetite Status", height=300, margin=dict(l=10, r=10, t=40, b=10))
    return fig

def create_treatment_gauge(confidence: float) -> go.Figure:
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=confidence,
        title={"text": "Treatment Confidence", "font": {"size": 14}},
        domain={"x": [0, 1], "y": [0, 1]},
        gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#6C63FF"},
               "steps": [{"range": [0, 50], "color": "#FEE2E2"}, {"range": [50, 75], "color": "#FEF3C7"}, {"range": [75, 100], "color": "#DCFCE7"}],
               "threshold": {"line": {"color": "red", "width": 4}, "thickness": 0.75, "value": confidence}}))
    fig.update_layout(height=250, margin=dict(l=10, r=10, t=50, b=10))
    return fig

# =============================================================================
# UI COMPONENTS
# =============================================================================
def apply_custom_theme(primary: str, secondary: str) -> None:
    st.markdown(f"""
    <style>
    :root {{ --p-color: {primary}; --s-color: {secondary}; }}
    .metric-card {{ background:white; border-radius:12px; padding:1rem; border-top:4px solid var(--p-color); box-shadow:0 2px 8px rgba(0,0,0,0.05); }}
    .metric-label {{ font-size:0.8rem; color:#6B7A8A; }}
    .metric-value {{ font-size:1.8rem; font-weight:800; color:#12384D; }}
    .stButton > button {{ background: var(--p-color) !important; color:white !important; border-radius:10px !important; }}
    </style>
    """, unsafe_allow_html=True)

def render_parser_audit_panel():
    if st.session_state.parser_audit:
        with st.expander("🔍 Parser Audit & Diagnostics", expanded=False):
            st.markdown("**Ingestion Confidence Report**")
            audit = st.session_state.parser_audit
            col1, col2, col3 = st.columns(3)
            col1.metric("Files Processed", audit.get("total_files", 0))
            col2.metric("Rows Scanned", audit.get("total_rows_scanned", 0))
            col3.metric("Valid Risks Extracted", audit.get("unique_risks", 0))
            col1, col2, col3 = st.columns(3)
            col1.metric("Taxonomy Rows Filtered", audit.get("total_filtered_taxonomy", 0))
            col2.metric("Header Rows Filtered", audit.get("total_filtered_header", 0))
            col3.metric("Short Rows Filtered", audit.get("total_filtered_short", 0))
            confidence = "High" if audit.get("unique_risks", 0) > 10 else "Medium" if audit.get("unique_risks", 0) > 3 else "Low"
            st.info(f"**Extraction Confidence:** {confidence} – {audit.get('unique_risks', 0)} validated risk statements")

def render_sidebar():
    with st.sidebar:
        st.markdown("## 🛡️ RiskForge")
        st.caption(f"**Tier:** {st.session_state.tier.upper()}")
        uploaded_logo = st.file_uploader("Company Logo", type=["png", "jpg", "jpeg"], key="logo_upload")
        if uploaded_logo:
            st.session_state.logo_bytes = uploaded_logo.getvalue()
        if st.session_state.tier == "free":
            st.markdown("---")
            st.markdown("### 🚀 Upgrade")
            st.markdown("""
**Professional** – $29/month or $99/year  
- Full board pack  
- AI briefing  
- Heatmaps  
- Category/Division charts  
""")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("$29/mo", key="pro_monthly"):
                    if stripe and STRIPE_PRICE_ID_PRO_MONTHLY:
                        session = stripe.checkout.Session.create(
                            payment_method_types=["card"],
                            line_items=[{"price": STRIPE_PRICE_ID_PRO_MONTHLY, "quantity": 1}],
                            mode="subscription",
                            success_url=APP_URL + "?success_pro_monthly=true",
                            cancel_url=APP_URL,
                        )
                        st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
            with col2:
                if st.button("$99/yr", key="pro_annual"):
                    if stripe and STRIPE_PRICE_ID_PRO_ANNUAL:
                        session = stripe.checkout.Session.create(
                            payment_method_types=["card"],
                            line_items=[{"price": STRIPE_PRICE_ID_PRO_ANNUAL, "quantity": 1}],
                            mode="subscription",
                            success_url=APP_URL + "?success_pro_annual=true",
                            cancel_url=APP_URL,
                        )
                        st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
            st.markdown("")
            st.markdown("""
**Enterprise** – $99/month or $299/year  
- Branded PDF board pack  
- Committee-ready exports  
- White-label reports  
- Priority support  
- Custom appetite thresholds  
""")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("$99/mo", key="ent_monthly"):
                    if stripe and STRIPE_PRICE_ID_ENT_MONTHLY:
                        session = stripe.checkout.Session.create(
                            payment_method_types=["card"],
                            line_items=[{"price": STRIPE_PRICE_ID_ENT_MONTHLY, "quantity": 1}],
                            mode="subscription",
                            success_url=APP_URL + "?success_ent_monthly=true",
                            cancel_url=APP_URL,
                        )
                        st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
            with col2:
                if st.button("$299/yr", key="ent_annual"):
                    if stripe and STRIPE_PRICE_ID_ENT_ANNUAL:
                        session = stripe.checkout.Session.create(
                            payment_method_types=["card"],
                            line_items=[{"price": STRIPE_PRICE_ID_ENT_ANNUAL, "quantity": 1}],
                            mode="subscription",
                            success_url=APP_URL + "?success_ent_annual=true",
                            cancel_url=APP_URL,
                        )
                        st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
            st.markdown("---")
            code = st.text_input("Unlock code", type="password")
            if code == PRO_UNLOCK_CODE:
                st.session_state.tier = "professional"
                st.rerun()
            if code == ENT_UNLOCK_CODE:
                st.session_state.tier = "enterprise"
                st.rerun()
        else:
            st.success(f"✅ {st.session_state.tier.upper()} active")
        st.markdown("---")
        st.session_state.org_name = st.text_input("Organization Name", st.session_state.org_name)
        st.session_state.report_title = st.text_input("Report Title", st.session_state.report_title)
        st.session_state.board_threshold = st.slider("Global Board Threshold", 0, 25, st.session_state.board_threshold)
        st.session_state.default_residual_score = st.slider("Default residual score (when missing)", 0, 25, st.session_state.default_residual_score)
        if st.session_state.tier == "enterprise":
            st.markdown("**Per-Category Appetite (Enterprise)**")
            categories = ["Strategic", "Financial", "Operational", "ICT/Cyber", "Compliance/Legal", "People/HR", "Health/Safety", "Reputational", "Environmental"]
            for cat in categories:
                current = st.session_state.category_appetite.get(cat, st.session_state.board_threshold)
                new_val = st.number_input(f"{cat}", min_value=0, max_value=25, value=current, key=f"appetite_{cat}")
                if new_val != current:
                    st.session_state.category_appetite[cat] = new_val
        st.session_state.primary_color = st.color_picker("Primary Color", st.session_state.primary_color)
        st.session_state.secondary_color = st.color_picker("Secondary Color", st.session_state.secondary_color)
        st.checkbox("🔧 Debug Mode (show parser details)", key="debug_mode")

# =============================================================================
# MAIN APP
# =============================================================================
def main():
    apply_custom_theme(st.session_state.primary_color, st.session_state.secondary_color)
    render_sidebar()

    # Hero banner – no raw HTML
    col_logo, col_text = st.columns([1, 5])
    with col_logo:
        if st.session_state.logo_bytes:
            st.image(st.session_state.logo_bytes, width=60)
        else:
            st.markdown("### 🛡️")
    with col_text:
        st.title(st.session_state.org_name)
        st.caption(st.session_state.report_title)
    
    render_parser_audit_panel()
    
    if st.session_state.tier == "free":
        st.info("🔓 Free tier: 1 file, preview only. Upgrade for full features.")
    
    max_files = 1 if st.session_state.tier == "free" else 999
    uploaded_files = st.file_uploader("Upload risk registers (Excel/CSV)", accept_multiple_files=True, type=["xlsx", "xls", "csv"])
    
    if len(uploaded_files) > max_files:
        st.error(f"Free tier allows {max_files} file(s).")
        uploaded_files = uploaded_files[:max_files]
    
    if st.button("🚀 Generate Board Pack", type="primary", use_container_width=True):
        if not uploaded_files:
            st.warning("Please upload files.")
        else:
            with st.spinner("Processing risk registers..."):
                df_all, debug_list = parse_all_files(uploaded_files, st.session_state.tier, st.session_state.default_residual_score)
                if df_all.empty:
                    st.error("No valid risk data found.")
                    if st.session_state.debug_mode:
                        st.subheader("🔧 Parser Debug Information")
                        for i, debug in enumerate(debug_list):
                            st.markdown(f"**File {i+1}**")
                            st.json(debug)
                else:
                    for debug in debug_list:
                        if debug.get("confidence_level") == "Low":
                            st.warning(f"⚠️ Parser confidence low for one file. Inferred columns: {debug.get('inferred_columns', [])}")
                    
                    category_appetite = st.session_state.category_appetite if st.session_state.tier == "enterprise" else None
                    snapshot = build_intelligence_snapshot(df_all, st.session_state.board_threshold, category_appetite)
                    comparison = {}
                    if st.session_state.history:
                        comparison = compare_snapshots(snapshot, st.session_state.history[-1])
                    ai_summary = ""
                    if st.session_state.tier != "free" and GEMINI_AVAILABLE:
                        safe_snapshot = make_json_serializable(snapshot)
                        ai_summary = cached_ai_summary(json.dumps(safe_snapshot), st.session_state.org_name)
                    narrative = generate_board_narrative(snapshot, comparison, st.session_state.board_threshold, st.session_state.org_name, st.session_state.report_title, ai_summary)
                    st.session_state.rf_data = {
                        "risks_df": df_all,
                        "total_risks": len(df_all),
                        "company": st.session_state.org_name,
                        "report_title": st.session_state.report_title,
                        "period": f"Q{((datetime.now().month-1)//3)+1} {datetime.now().year}",
                        "board_date": datetime.now().strftime("%B %d, %Y"),
                        "threshold": st.session_state.board_threshold,
                        "critical_count": snapshot.get("critical_count", 0),
                        "high_count": snapshot.get("high_count", 0),
                        "avg_residual": snapshot.get("avg_residual", 0),
                        "avg_inherent": snapshot.get("avg_inherent", 0),
                        "enterprise_health_score": snapshot.get("enterprise_health_score", 0),
                        "treatment_confidence": snapshot.get("treatment_confidence", 0),
                        "top_division": snapshot.get("top_division", "N/A"),
                        "top_division_pct": snapshot.get("top_division_pct", 0),
                        "division_exposure": snapshot.get("division_exposure", {}),
                        "category_exposure": snapshot.get("category_exposure", {}),
                        "ownership_coverage": snapshot.get("ownership_coverage", 0),
                        "pct_within_appetite": snapshot.get("pct_within_appetite", 0),
                        "pct_near_appetite": snapshot.get("pct_near_appetite", 0),
                        "pct_breached": snapshot.get("pct_breached", 0),
                        "emerging_themes": snapshot.get("emerging_themes", []),
                        "board_risks": snapshot.get("board_risks", []),
                        "narrative": narrative,
                        "comparison": comparison,
                        "ai_summary": ai_summary
                    }
                    st.session_state.history.append(snapshot)
                    if len(st.session_state.history) > 4:
                        st.session_state.history = st.session_state.history[-4:]
                    st.success(f"✅ {len(df_all)} risks processed")
                    if st.session_state.tier != "free":
                        excel_data = generate_excel_pack(st.session_state.rf_data, narrative)
                        st.download_button("📥 Excel Board Pack", excel_data, file_name=f"RiskForge_{datetime.now().strftime('%Y%m%d')}.xlsx")
                        if st.session_state.tier == "enterprise":
                            pdf_data = generate_pdf_board_pack(narrative, snapshot, st.session_state.org_name, st.session_state.report_title, st.session_state.logo_bytes)
                            st.download_button("📥 PDF Board Pack (Enterprise)", pdf_data, file_name=f"BoardPack_{datetime.now().strftime('%Y%m%d')}.pdf")
                    else:
                        st.info("📌 Upgrade to Professional/Enterprise to download board packs.")
    
    if st.session_state.rf_data:
        data = st.session_state.rf_data
        df = data["risks_df"]
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "📑 Register", "🧠 Intelligence", "📈 Trends", "📤 Export"])
        with tab1:
            st.subheader("Executive Dashboard")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Total Risks", data["total_risks"])
            col2.metric("Critical+High", data["critical_count"] + data["high_count"])
            col3.metric("Health Score", f"{data['enterprise_health_score']}/100")
            col4.metric("Treatment Confidence", f"{data.get('treatment_confidence', 0)}%")
            col5.metric("Top Division", f"{data['top_division']} ({data['top_division_pct']}%)")
            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                if not df.empty:
                    fig_cat = create_category_chart(df)
                    st.plotly_chart(fig_cat, use_container_width=True)
            with col_chart2:
                if not df.empty:
                    fig_div = create_division_chart(df)
                    st.plotly_chart(fig_div, use_container_width=True)
            col_app, col_treat = st.columns(2)
            with col_app:
                if not df.empty:
                    fig_app = create_appetite_gauge(df, data["threshold"], st.session_state.category_appetite if st.session_state.tier == "enterprise" else None)
                    st.plotly_chart(fig_app, use_container_width=True)
            with col_treat:
                fig_treat = create_treatment_gauge(data.get("treatment_confidence", 0))
                st.plotly_chart(fig_treat, use_container_width=True)
            st.subheader("Risk Appetite Status")
            st.progress(data.get("pct_within_appetite", 0) / 100)
            st.caption(f"Within: {data.get('pct_within_appetite', 0)}% | Near: {data.get('pct_near_appetite', 0)}% | Breached: {data.get('pct_breached', 0)}%")
        with tab2:
            st.subheader("Enterprise Risk Register")
            st.dataframe(df, use_container_width=True)
        with tab3:
            st.subheader("Board Intelligence Report")
            if data.get("ai_summary"):
                st.info(f"**AI Executive Summary:** {data['ai_summary']}")
            st.markdown(data.get("narrative", "No narrative available"))
            if data.get("emerging_themes"):
                st.markdown("**Emerging Themes:** " + ", ".join(data["emerging_themes"]))
            if data.get("board_risks"):
                st.markdown("**Top Board-Attention Risks**")
                for risk in data["board_risks"]:
                    st.markdown(f"- **{risk['risk_name']}** – Residual: {risk['residual_score']}/25, Owner: {risk['owner']}")
        with tab4:
            st.subheader("Quarter Comparison & Trends")
            if data.get("comparison"):
                comp = data["comparison"]
                if comp.get("new_risks"):
                    st.markdown(f"**🆕 New Risks:** {len(comp['new_risks'])}")
                if comp.get("closed_risks"):
                    st.markdown(f"**✅ Closed Risks:** {len(comp['closed_risks'])}")
                if comp.get("worsened_risks"):
                    st.markdown(f"**📈 Worsened Risks:** {len(comp['worsened_risks'])}")
                    for r in comp["worsened_risks"][:5]:
                        st.markdown(f"- {r['name']} (+{r['delta']})")
                if comp.get("improved_risks"):
                    st.markdown(f"**📉 Improved Risks:** {len(comp['improved_risks'])}")
                if comp.get("health_delta", 0) != 0:
                    direction = "improved" if comp["health_delta"] > 0 else "declined"
                    st.markdown(f"**Health Score Change:** {abs(comp['health_delta']):.1f} points {direction}")
                if comp.get("appetite_delta", 0) != 0:
                    direction = "worsened" if comp["appetite_delta"] > 0 else "improved"
                    st.markdown(f"**Appetite Breach Change:** {abs(comp['appetite_delta']):.1f}% {direction}")
            else:
                st.info("Upload another register next quarter to see trend analysis.")
        with tab5:
            st.subheader("Export Options")
            if st.session_state.tier != "free":
                st.success("✅ Professional/Enterprise tier – full export available above.")
                if st.session_state.tier == "enterprise":
                    st.caption("Enterprise tier includes branded PDF board packs with your organization's logo and custom report titles.")
            else:
                st.info("📌 Upgrade to Professional to download board packs.")

if __name__ == "__main__":
    main()