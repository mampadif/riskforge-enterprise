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
from openpyxl.styles import PatternFill
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.units import inch
from rapidfuzz import fuzz

# Optional imports
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    EMBEDDING_AVAILABLE = True
except ImportError:
    EMBEDDING_AVAILABLE = False

# =============================================================================
# CONFIGURATION
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
if "force_gemini" not in st.session_state:
    st.session_state.force_gemini = True

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
# HELPER FUNCTIONS
# =============================================================================
def clean_division_name(filename: str) -> str:
    name = re.sub(r"\.xlsx$|\.xls$|\.csv$", "", filename, flags=re.IGNORECASE)
    name = re.sub(r"^copy of\s+", "", name, flags=re.IGNORECASE)
    name = name.replace("_", " ").strip()
    name = re.sub(r"\s+", " ", name)
    return name.title() if name else "Unknown Division"

def parse_risk_score(val: Any) -> Optional[float]:
    """Convert various text/number representations to a 1-5 numeric score."""
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    if s in ["#n/a", "#value!", "#ref!", "#name?", "#num!", "#null!", "none", "nan", "", "0"]:
        return None
    
    # Direct text mappings from the Boundaries sheet
    impact_map = {
        "critical": 5, "major": 4, "moderate": 3, "significant": 2, "minor": 1
    }
    likelihood_map = {
        "almost certain": 5, "likely": 4, "moderate": 3, "unlikely": 2, "rare": 1
    }
    if s in impact_map:
        return float(impact_map[s])
    if s in likelihood_map:
        return float(likelihood_map[s])
    
    # Try numeric extraction
    match = re.search(r'(\d+(?:\.\d+)?)', s)
    if match:
        num = float(match.group(1))
        if 1 <= num <= 5:
            return num
        elif 1 <= num <= 25:
            return round(num / 5)
    return None

def parse_control_effectiveness(val: Any) -> Optional[int]:
    """Convert control effectiveness text to numeric 1-5 scale (1=Very Good, 5=Unsatisfactory)."""
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    mapping = {
        "very good": 1, "good": 2, "satisfactory": 3, "weak": 4, "unsatisfactory": 5
    }
    for key, num in mapping.items():
        if key in s:
            return num
    match = re.search(r'(\d+)', s)
    if match:
        num = int(match.group(1))
        if 1 <= num <= 5:
            return num
    return None

# =============================================================================
# ENHANCED CELL VALUE EXTRACTION (HANDLES FORMULAS & MERGED CELLS)
# =============================================================================
def get_cell_value(ws, row: int, col: int) -> Any:
    """
    Safely retrieve a cell value from an openpyxl worksheet.
    - Handles merged cells (returns top-left value).
    - If data_only returns None but cell has a formula, try to extract a sensible
      value from the formula string (e.g., a VLOOKUP reference).
    - Returns None if nothing useful can be extracted.
    """
    cell = ws.cell(row=row, column=col)
    
    # Handle merged cells
    if cell.coordinate in ws.merged_cells:
        for merged_range in ws.merged_cells.ranges:
            if cell.coordinate in merged_range:
                # Get the top-left cell of the merged range
                min_col, min_row, max_col, max_row = merged_range.bounds
                cell = ws.cell(row=min_row, column=min_col)
                break
    
    val = cell.value
    
    # If data_only gave us a string that looks like an error, treat as None
    if isinstance(val, str) and val.startswith("#"):
        val = None
    
    # If value is None, try to extract from formula
    if val is None and cell.data_type == 'f':
        formula = cell.value  # actually the formula string
        if formula:
            # Try to extract a reference like "Boundaries!$A$19:$B$24"
            match = re.search(r'!(\$?[A-Z]+\$?\d+)', formula)
            if match:
                ref = match.group(1)
                try:
                    ref_cell = ws[ref]
                    val = ref_cell.value
                except:
                    pass
    return val

def parse_risk_score_from_text(val: Any) -> Optional[float]:
    """Enhanced score parser that also handles formula text like =VLOOKUP(...)."""
    if val is None:
        return None
    s = str(val).strip()
    if s.startswith("="):
        num_match = re.search(r'\b(\d+(?:\.\d+)?)\b', s)
        if num_match:
            return float(num_match.group(1))
        return None
    return parse_risk_score(val)

def parse_control_effectiveness_text(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s.startswith("="):
        num_match = re.search(r'\b([1-5])\b', s)
        if num_match:
            return int(num_match.group(1))
        return None
    return parse_control_effectiveness(val)

def infer_category_from_text(text: str) -> str:
    """Infer risk category based on keywords."""
    t = text.lower()
    if any(k in t for k in ["compliance", "regulation", "labour law", "employment act", "statute"]):
        return "Compliance/Legal"
    elif any(k in t for k in ["financial", "budget", "funding", "revenue"]):
        return "Financial"
    elif any(k in t for k in ["cyber", "data", "it", "information security", "hack"]):
        return "ICT/Cyber"
    elif any(k in t for k in ["staff", "employee", "turnover", "morale", "talent", "training", "development"]):
        return "People/HR"
    elif any(k in t for k in ["safety", "health", "injury", "accident", "she"]):
        return "Health/Safety"
    elif any(k in t for k in ["reputation", "brand"]):
        return "Reputational"
    elif any(k in t for k in ["operational", "process", "disruption"]):
        return "Operational"
    elif any(k in t for k in ["strategic", "mandate"]):
        return "Strategic"
    return "Uncategorised"

# =============================================================================
# ENHANCED DIRECT HR RISK REGISTER PARSER (SCANS ALL SHEETS)
# =============================================================================
def parse_hr_risk_register_direct(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    """
    Robust parser that extracts risks from:
    - The 'consolidated risk register' sheet (table format)
    - Any sheet with 'Name of risk' marker (individual monitoring tool sheets)
    Handles formulas, merged cells, and variable column positions.
    """
    risks = []
    debug_info = {
        "sheets_scanned": 0,
        "rows_processed": 0,
        "risks_found": 0,
        "skipped_rows": [],
        "errors": []
    }
    
    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True)
        
        # ---- First, scan all sheets for the "Name of risk" pattern (monitoring tools) ----
        for sheet_name in wb.sheetnames:
            if sheet_name.lower() in ["boundaries", "impact", "likelihood", "effectiveness", "risk matrix", "sample mt"]:
                continue
            ws = wb[sheet_name]
            debug_info["sheets_scanned"] += 1
            
            # Strategy A: Find the cell containing "Name of risk"
            name_of_risk_row = None
            name_of_risk_col = None
            for row_idx in range(1, min(30, ws.max_row + 1)):
                for col_idx in range(1, min(20, ws.max_column + 1)):
                    cell_val = get_cell_value(ws, row_idx, col_idx)
                    if cell_val and isinstance(cell_val, str) and "name of risk" in cell_val.lower():
                        name_of_risk_row = row_idx
                        name_of_risk_col = col_idx
                        break
                if name_of_risk_row:
                    break
            
            if name_of_risk_row:
                # The risk name is usually in column H (8) on the same row
                risk_name_cell = get_cell_value(ws, name_of_risk_row, 8)
                if not risk_name_cell:
                    continue
                risk_name = str(risk_name_cell).strip()
                if not risk_name or len(risk_name) < 3:
                    continue
                
                # Find description: usually a few rows down in column H
                risk_desc = ""
                for r_offset in range(1, 5):
                    desc_val = get_cell_value(ws, name_of_risk_row + r_offset, 8)
                    if desc_val and len(str(desc_val).strip()) > 20:
                        risk_desc = str(desc_val).strip()
                        break
                if not risk_desc:
                    risk_desc = risk_name  # fallback
                
                # Impact and likelihood: search nearby rows
                impact_score = None
                likelihood_score = None
                for r in range(name_of_risk_row, min(name_of_risk_row + 15, ws.max_row + 1)):
                    for c in range(1, min(20, ws.max_column + 1)):
                        val = get_cell_value(ws, r, c)
                        if val and isinstance(val, str):
                            if "impact rating:" in val.lower():
                                imp_val = get_cell_value(ws, r, c + 1)
                                impact_score = parse_risk_score_from_text(imp_val)
                            elif "likelihood rating" in val.lower():
                                like_val = get_cell_value(ws, r, c + 1)
                                likelihood_score = parse_risk_score_from_text(like_val)
                
                # Control effectiveness
                control_eff_text = "Not rated"
                control_eff_numeric = None
                for r in range(name_of_risk_row, min(name_of_risk_row + 20, ws.max_row + 1)):
                    for c in range(1, min(20, ws.max_column + 1)):
                        val = get_cell_value(ws, r, c)
                        if val and isinstance(val, str) and "overall effectiveness" in val.lower():
                            ctrl_val = get_cell_value(ws, r, c + 1)
                            control_eff_numeric = parse_control_effectiveness_text(ctrl_val)
                            control_eff_text = str(ctrl_val).strip() if ctrl_val else "Not rated"
                            break
                
                # Owner
                owner = "Not assigned"
                for r in range(name_of_risk_row, min(name_of_risk_row + 20, ws.max_row + 1)):
                    for c in range(1, min(20, ws.max_column + 1)):
                        val = get_cell_value(ws, r, c)
                        if val and isinstance(val, str) and "risk owner" in val.lower():
                            own_val = get_cell_value(ws, r, c + 1)
                            if own_val:
                                owner = str(own_val).strip()
                            break
                
                # Division inference
                division = clean_division_name(file_name)
                if "human" in sheet_name.lower() or "hr" in sheet_name.lower():
                    division = "Human Resources"
                elif "it" in sheet_name.lower() or "technologies" in sheet_name.lower():
                    division = "Information Technology"
                elif "r&p" in sheet_name.lower():
                    division = "Research & Partnerships"
                elif "f&o" in sheet_name.lower():
                    division = "Finance & Operations"
                elif "nrm" in sheet_name.lower():
                    division = "Natural Resources"
                elif "ceo" in sheet_name.lower():
                    division = "Executive"
                
                # Inherent & residual
                inherent = default_residual
                if impact_score is not None and likelihood_score is not None:
                    inherent = min(25, max(1, impact_score * likelihood_score))
                elif impact_score is not None:
                    inherent = min(25, impact_score * 5)
                elif likelihood_score is not None:
                    inherent = min(25, likelihood_score * 5)
                
                residual = inherent
                if control_eff_numeric is not None:
                    factor = control_eff_numeric / 5.0
                    residual = round(inherent * factor)
                
                category = infer_category_from_text(risk_desc)
                
                risks.append({
                    "division": division,
                    "division_confidence": 0.95,
                    "division_source": "monitoring_sheet_parser",
                    "risk_name": risk_name[:80],
                    "risk_statement": risk_desc[:500],
                    "category": category,
                    "residual_score": min(25, max(1, residual)),
                    "inherent_score": min(25, max(1, inherent)),
                    "owner": owner,
                    "status": "Active",
                    "due_date": None,
                    "control_effectiveness": control_eff_text,
                    "impact_score": impact_score,
                    "likelihood_score": likelihood_score,
                })
                debug_info["risks_found"] += 1
        
        # ---- Strategy B: Process the consolidated risk register table ----
        for sheet_name in wb.sheetnames:
            if "consolidated risk register" not in sheet_name.lower():
                continue
            ws = wb[sheet_name]
            debug_info["sheets_scanned"] += 1
            
            # Find header row (contains "RISK DESCRIPTION")
            header_row = None
            for row_idx in range(1, min(20, ws.max_row + 1)):
                for col_idx in range(1, min(30, ws.max_column + 1)):
                    val = get_cell_value(ws, row_idx, col_idx)
                    if val and isinstance(val, str) and "risk description" in val.lower():
                        header_row = row_idx
                        break
                if header_row:
                    break
            
            if not header_row:
                debug_info["errors"].append(f"Sheet '{sheet_name}' has no 'RISK DESCRIPTION' header")
                continue
            
            # Identify columns by scanning header row
            col_risk_name = None      # Column C (short name)
            col_risk_desc = None      # Column D (full statement)
            col_impact = None         # Column G
            col_likelihood = None     # Column I
            col_control = None        # Column N
            col_owner = None          # Column O
            
            for c in range(1, min(30, ws.max_column + 1)):
                hdr = get_cell_value(ws, header_row, c)
                if not hdr or not isinstance(hdr, str):
                    continue
                hdr_low = hdr.lower()
                if "risk description" in hdr_low and "(" in hdr:
                    # This is column C (the short label)
                    col_risk_name = c
                elif "risk definition" in hdr_low or "risk statement" in hdr_low:
                    # This is column D (the full statement)
                    col_risk_desc = c
                elif "impact" in hdr_low and "(" in hdr:
                    col_impact = c
                elif "likelihood" in hdr_low and "(" in hdr:
                    col_likelihood = c
                elif "control effectiveness" in hdr_low:
                    col_control = c
                elif "risk owner" in hdr_low:
                    col_owner = c
            
            # Fallback: if we didn't find column D, try to infer from positions
            if not col_risk_desc:
                # Assume column D is right after column C
                if col_risk_name:
                    col_risk_desc = col_risk_name + 1
                else:
                    col_risk_desc = 4  # Default to column D
            
            if not col_risk_desc:
                debug_info["errors"].append(f"Could not find risk description column in '{sheet_name}'")
                continue
            
            # Process rows after header
            for row_idx in range(header_row + 1, ws.max_row + 1):
                debug_info["rows_processed"] += 1
                
                # Get risk name (short label from column C)
                name_val = get_cell_value(ws, row_idx, col_risk_name) if col_risk_name else None
                risk_name = str(name_val).strip() if name_val else ""
                
                # Get risk description (full statement from column D)
                desc_val = get_cell_value(ws, row_idx, col_risk_desc) if col_risk_desc else None
                if not desc_val:
                    debug_info["skipped_rows"].append({
                        "sheet": sheet_name,
                        "row": row_idx,
                        "reason": "Empty description cell"
                    })
                    continue
                
                desc_str = str(desc_val).strip()
                # Skip only if completely empty or a header word
                if not desc_str or desc_str.lower() in ["risk definition", "risk statement", "nan", "none"]:
                    continue
                # Do NOT skip based on length – Staff Turnover is a valid short name
                
                # If we didn't get a name from column C, fall back to first 50 chars of description
                if not risk_name:
                    risk_name = desc_str[:50]
                
                # Impact (column G)
                impact_val = get_cell_value(ws, row_idx, col_impact) if col_impact else None
                impact_score = parse_risk_score_from_text(impact_val)
                
                # Likelihood (column I)
                likelihood_val = get_cell_value(ws, row_idx, col_likelihood) if col_likelihood else None
                likelihood_score = parse_risk_score_from_text(likelihood_val)
                
                # Control effectiveness (column N)
                control_val = get_cell_value(ws, row_idx, col_control) if col_control else None
                control_eff_numeric = parse_control_effectiveness_text(control_val)
                control_eff_text = str(control_val).strip() if control_val else "Not rated"
                
                # Owner (column O)
                owner_val = get_cell_value(ws, row_idx, col_owner) if col_owner else None
                owner = str(owner_val).strip() if owner_val and str(owner_val).lower() != "nan" else "Not assigned"
                
                # Scores
                inherent = default_residual
                if impact_score is not None and likelihood_score is not None:
                    inherent = min(25, max(1, impact_score * likelihood_score))
                elif impact_score is not None:
                    inherent = min(25, impact_score * 5)
                elif likelihood_score is not None:
                    inherent = min(25, likelihood_score * 5)
                
                residual = inherent
                if control_eff_numeric is not None:
                    factor = control_eff_numeric / 5.0
                    residual = round(inherent * factor)
                
                division = "Human Resources"
                category = infer_category_from_text(desc_str)
                
                risks.append({
                    "division": division,
                    "division_confidence": 0.95,
                    "division_source": "consolidated_register_parser",
                    "risk_name": risk_name,
                    "risk_statement": desc_str[:500],
                    "category": category,
                    "residual_score": min(25, max(1, residual)),
                    "inherent_score": min(25, max(1, inherent)),
                    "owner": owner,
                    "status": "Active",
                    "due_date": None,
                    "control_effectiveness": control_eff_text,
                    "impact_score": impact_score,
                    "likelihood_score": likelihood_score,
                })
                debug_info["risks_found"] += 1
        
        if risks:
            df = pd.DataFrame(risks)
            # Remove duplicates by risk statement (keep first)
            df = df.drop_duplicates(subset=["risk_statement"], keep="first")
            return df, debug_info
        else:
            debug_info["errors"].append("No risks found in any sheet")
            return pd.DataFrame(), debug_info
            
    except Exception as e:
        debug_info["errors"].append(str(e))
        return pd.DataFrame(), debug_info

# =============================================================================
# MAIN PARSER DISPATCHER
# =============================================================================
def parse_uploaded_file_bytes(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    # Use the enhanced multi-sheet parser
    df, debug = parse_hr_risk_register_direct(file_bytes, file_name, default_residual)
    if not df.empty:
        st.success(f"✅ Extracted {len(df)} risks from all sheets")
        return df, debug
    
    # Fallback to Gemini if nothing found
    if GEMINI_AVAILABLE and st.session_state.force_gemini:
        df, debug = gemini_extract_risks(file_bytes, file_name, default_residual)
        if not df.empty:
            st.success(f"✅ Gemini extracted {len(df)} risks")
            return df, debug
    
    return pd.DataFrame(), {"error": "No risks could be extracted"}

def parse_all_files(uploaded_files, tier: str, default_residual: int) -> Tuple[pd.DataFrame, List[Dict]]:
    all_risks = []
    all_debug = []
    for file in uploaded_files:
        df, debug = cached_parse_file(file.getvalue(), file.name, default_residual)
        all_debug.append(debug)
        if not df.empty:
            all_risks.append(df)
    if not all_risks:
        return pd.DataFrame(), all_debug
    df_all = pd.concat(all_risks, ignore_index=True)
    df_all, _ = detect_semantic_duplicates(df_all, threshold=0.85)
    df_all["residual_level"] = df_all["residual_score"].apply(lambda x: "Critical" if x >= 20 else "High" if x >= 12 else "Medium" if x >= 6 else "Low")
    if tier == "free":
        df_all = df_all.head(10)
    st.session_state.parser_audit = {"total_files": len(uploaded_files), "total_risks": len(df_all)}
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
    except:
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
# AI FUNCTIONS
# =============================================================================
def ai_infer_category(statement: str, fallback: str = "Uncategorised") -> str:
    categories = ["Strategic", "Financial", "Operational", "ICT/Cyber", "Compliance/Legal", "People/HR", "Health/Safety", "Reputational", "Environmental"]
    if not GEMINI_AVAILABLE or len(statement) < 20:
        return fallback
    try:
        prompt = f"Classify this risk into exactly one of these categories: {', '.join(categories)}. Return ONLY the category name.\nRisk: {statement[:400]}"
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
Be concise, actionable."""
        response = ai_model.generate_content(prompt)
        return response.text.strip() if response.text else ""
    except:
        return ""

def gemini_extract_risks(file_bytes: bytes, file_name: str, default_residual: int) -> Tuple[pd.DataFrame, Dict]:
    if not GEMINI_AVAILABLE:
        return pd.DataFrame(), {"error": "Gemini not available"}
    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
        all_content = []
        for sheet in xls.sheet_names:
            if sheet.lower() in ["boundaries", "impact", "likelihood", "effectiveness", "risk matrix"]:
                continue
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)
            sheet_str = f"\n[SHEET: {sheet}]\n"
            for i in range(min(50, df.shape[0])):
                row_vals = []
                for j in range(min(20, df.shape[1])):
                    val = df.iat[i, j]
                    if pd.notna(val) and str(val).strip():
                        row_vals.append(str(val).strip())
                if row_vals:
                    sheet_str += f"Row {i+1}: {' | '.join(row_vals)}\n"
            all_content.append(sheet_str)
        content = "\n".join(all_content)[:8000]
        prompt = f"""
You are a risk extraction engine. Extract ALL risks as a JSON list.
Each risk must have: risk_statement, risk_name, owner, residual_score (1-25).
Return ONLY valid JSON.
Content:
{content}
"""
        response = ai_model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```json"): text = text[7:]
        if text.startswith("```"): text = text[3:]
        if text.endswith("```"): text = text[:-3]
        data = json.loads(text)
        risks = data.get("risks", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        rows = []
        for r in risks:
            stmt = r.get("risk_statement", "")
            if not stmt or len(stmt) < 15:
                continue
            rows.append({
                "division": clean_division_name(file_name),
                "division_confidence": 0.8,
                "division_source": "gemini",
                "risk_name": r.get("risk_name", stmt[:50]),
                "risk_statement": stmt[:500],
                "category": "Uncategorised",
                "residual_score": min(25, max(1, int(r.get("residual_score", default_residual)))),
                "inherent_score": min(25, int(r.get("residual_score", default_residual)) + 3),
                "owner": r.get("owner", "Not assigned"),
                "status": "Active",
                "due_date": None,
                "control_effectiveness": "Not rated",
                "impact_score": r.get("impact_score"),
                "likelihood_score": r.get("likelihood_score"),
            })
        if rows:
            return pd.DataFrame(rows), {"extracted": len(rows), "method": "gemini"}
        return pd.DataFrame(), {"error": "No risks found"}
    except Exception as e:
        return pd.DataFrame(), {"error": f"Gemini failed: {str(e)}"}

# =============================================================================
# INTELLIGENCE ENGINE
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
        "Talent & Workforce": ["staff", "skills", "turnover", "recruitment", "retention", "morale", "attrition"],
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
            col1, col2 = st.columns(2)
            col1.metric("Files Processed", audit.get("total_files", 0))
            col2.metric("Total Risks", audit.get("total_risks", 0))

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
        st.checkbox("🔧 Debug Mode", key="debug_mode")
        if GEMINI_AVAILABLE:
            st.checkbox("🤖 Force Gemini Extraction", key="force_gemini", value=True)

# =============================================================================
# MAIN APP
# =============================================================================
def main():
    apply_custom_theme(st.session_state.primary_color, st.session_state.secondary_color)
    render_sidebar()

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
    
    if GEMINI_AVAILABLE:
        st.success("✅ Gemini API is configured and ready")
    else:
        st.error("❌ Gemini API key not found")
    
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
                    st.success(f"✅ {len(df_all)} risks processed")
                    if st.session_state.debug_mode:
                        with st.expander("🔧 Parsed Risks Preview"):
                            st.dataframe(df_all.head(20))
                    
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