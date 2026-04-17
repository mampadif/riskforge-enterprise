import streamlit as st
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
import os
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
from rapidfuzz import fuzz, process

# Optional sentence-transformers for semantic deduplication
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    EMBEDDING_AVAILABLE = True
except ImportError:
    EMBEDDING_AVAILABLE = False

# =============================================================================
# HEALTH SCORE WEIGHTS (adjustable, not hardcoded)
# =============================================================================
HEALTH_WEIGHTS = {
    "exposure": 0.40,           # residual exposure burden
    "control": 0.25,            # control effectiveness
    "treatment": 0.20,          # treatment delivery confidence
    "appetite": 0.10,           # appetite compliance
    "concentration_penalty": 0.05  # penalty for concentration risk
}

# Posture thresholds (score -> label)
POSTURE_THRESHOLDS = [
    (80, "Strong"),
    (65, "Stable"),
    (50, "Elevated"),
    (0, "Attention Required")
]

# Concentration penalty triggers
CONCENTRATION_PENALTY_THRESHOLD = 0.4   # top division >40% of exposure triggers penalty
MAX_CONCENTRATION_PENALTY = 20          # max penalty points (out of 100)

# =============================================================================
# CONFIGURATION
# =============================================================================
st.set_page_config(page_title="RiskForge Enterprise", page_icon="🛡️", layout="wide")

# Stripe & secrets
STRIPE_SECRET_KEY = st.secrets.get("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID_PRO_MONTHLY = st.secrets.get("FORGE_STRIPE_PRICE_ID_PRO_MONTHLY")
STRIPE_PRICE_ID_PRO_ANNUAL = st.secrets.get("FORGE_STRIPE_PRICE_ID_PRO_ANNUAL")
STRIPE_PRICE_ID_ENT_MONTHLY = st.secrets.get("FORGE_STRIPE_PRICE_ID_ENT_MONTHLY")
STRIPE_PRICE_ID_ENT_ANNUAL = st.secrets.get("FORGE_STRIPE_PRICE_ID_ENT_ANNUAL")
APP_URL = st.secrets.get("APP_URL", "https://your-app.streamlit.app")
PRO_UNLOCK_CODE = st.secrets.get("PRO_UNLOCK_CODE", "PRO2025")
ENT_UNLOCK_CODE = st.secrets.get("ENT_UNLOCK_CODE", "ENT2025")

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

# =============================================================================
# UNIVERSAL PARSER HELPERS (unchanged, but included for completeness)
# =============================================================================
def normalize_text(val: Any) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    text = str(val).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text

def merged_cell_value(ws, row_idx: int, col_idx: int) -> Any:
    cell = ws.cell(row=row_idx, column=col_idx)
    if cell.value is not None:
        return cell.value

    for merged_range in ws.merged_cells.ranges:
        if (merged_range.min_row <= row_idx <= merged_range.max_row
                and merged_range.min_col <= col_idx <= merged_range.max_col):
            return ws.cell(merged_range.min_row, merged_range.min_col).value
    return None

def parse_due_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val

    text = normalize_text(val)
    if not text:
        return None

    for fmt in [
        "%B %d, %Y", "%b %d, %Y", "%b-%y", "%B-%y",
        "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d/%m/%Y",
        "%d-%b-%y", "%b-%Y",
    ]:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            pass

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return parsed.date()
    return None

def is_year_like(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    patterns = [
        r"^\d{4}[\-/ ]?\d{2}$",
        r"^\d{4}$",
        r"^\d{2}[\-/]\d{2}$",
    ]
    for pat in patterns:
        if re.match(pat, t):
            return True
    return False

def clean_division_value(val: Any) -> str:
    text = normalize_text(val)
    if not text:
        return ""

    if is_year_like(text):
        return ""

    text = re.sub(
        r"^(division\/dept|division|department|dept|directorate|function|unit)\s*[:\-]?\s*",
        "",
        text,
        flags=re.I
    )
    text = re.sub(r"\s+", " ", text).strip(" :-")

    abbrev_map = {
        "hr": "Human Resources",
        "it": "Information Technology",
        "ict": "Information Technology",
        "ist": "Information Systems and Technology",
        "nano": "Nanomaterials",
        "mobility": "eMobility",
    }

    key = text.lower()
    if key in abbrev_map:
        return abbrev_map[key]

    return text.title()

def looks_like_category(text: str) -> bool:
    categories = {
        "strategic", "financial", "operational", "compliance/legal", "people/hr",
        "ict/cyber", "reputational", "health/safety", "environmental", "legal", "hr", "uncategorised",
    }
    return normalize_text(text).lower() in categories

def infer_division_from_filename(file_name: str) -> Dict[str, Any]:
    if not file_name:
        return {"division": "Unknown Division", "confidence": 0.0, "source": "filename"}

    name = file_name.lower()
    patterns = {
        "Human Resources": [r"\bhr\b", r"human resources?"],
        "Information Systems and Technology": [r"\bist\b", r"information systems? and technology"],
        "Information Technology": [r"\bit\b", r"\bict\b", r"information technology"],
        "Nanomaterials": [r"\bnano\b", r"nanomaterials?"],
        "eMobility": [r"emobility", r"e-mobility", r"\bmobility\b"],
        "Finance": [r"\bfinance\b", r"financial"],
        "Legal": [r"\blegal\b"],
    }

    for division, pats in patterns.items():
        if any(re.search(p, name) for p in pats):
            return {"division": division, "confidence": 0.85, "source": "filename"}

    return {"division": "Unknown Division", "confidence": 0.0, "source": "filename"}

def detect_explicit_division(ws, header_row: Optional[int] = None, scan_cols: int = 12) -> Optional[Dict[str, Any]]:
    label_regex = re.compile(
        r"^(division\/dept|division|department|dept|directorate|function|unit|business unit)\s*[:\-]?$",
        re.I
    )

    max_row_to_scan = max(1, (header_row - 1) if header_row else 12)
    max_row_to_scan = min(max_row_to_scan, 15)

    for r in range(1, max_row_to_scan + 1):
        for c in range(1, min(scan_cols, ws.max_column) + 1):
            label = normalize_text(merged_cell_value(ws, r, c))
            if not label:
                continue

            if label_regex.match(label):
                candidates = []
                for offset in range(1, 6):
                    candidates.append(merged_cell_value(ws, r, c + offset))
                for offset in range(0, 6):
                    candidates.append(merged_cell_value(ws, r + 1, c + offset))

                for cand in candidates:
                    cand_text = clean_division_value(cand)
                    if not cand_text or len(cand_text) < 2:
                        continue
                    if is_year_like(cand_text):
                        continue
                    if looks_like_category(cand_text):
                        continue
                    reject_terms = [
                        "risk register", "risk description", "risk definition", "risk category",
                        "impact", "likelihood", "inherent risk", "controls", "owner", "cause",
                        "objective", "updated",
                    ]
                    if any(term in cand_text.lower() for term in reject_terms):
                        continue
                    return {"division": cand_text, "confidence": 0.95, "source": "explicit_cell"}
    return None

def get_division_for_risk(file_name: str, sheet_name: str, explicit: Optional[Dict[str, Any]]) -> Tuple[str, str, float]:
    if explicit:
        return explicit["division"], explicit["source"], explicit["confidence"]

    inferred = infer_division_from_filename(file_name)
    if inferred["confidence"] > 0:
        return inferred["division"], inferred["source"], inferred["confidence"]

    clean_sheet = re.sub(r"(?i)\brisk\s*register\b", "", sheet_name).strip()
    clean_sheet = re.sub(r"[_\-\s]+", " ", clean_sheet).strip()
    if clean_sheet and not is_year_like(clean_sheet) and len(clean_sheet) >= 2:
        return clean_sheet.title(), "sheet_name", 0.60

    return "Unknown Division", "fallback", 0.0

# =============================================================================
# VALID CATEGORIES
# =============================================================================
VALID_CATEGORIES = {
    "strategic",
    "financial",
    "operational",
    "compliance/legal",
    "people/hr",
    "ict/cyber",
    "reputational",
    "health/safety",
    "environmental",
}

def normalize_category_value(val: str) -> str:
    raw = normalize_text(val).strip()
    if not raw:
        return ""

    raw_lower = raw.lower()

    aliases = {
        "hr": "People/HR",
        "human resources": "People/HR",
        "compliance": "Compliance/Legal",
        "legal": "Compliance/Legal",
        "ict": "ICT/Cyber",
        "it": "ICT/Cyber",
        "cyber": "ICT/Cyber",
        "health and safety": "Health/Safety",
        "strategic": "Strategic",
        "financial": "Financial",
        "operational": "Operational",
        "reputational": "Reputational",
        "environmental": "Environmental",
    }

    if raw_lower in aliases:
        return aliases[raw_lower]

    for valid in VALID_CATEGORIES:
        if raw_lower == valid:
            return valid.title()

    return ""

def infer_category_from_text(title: str, statement: str, cause: str, raw_category: str = "") -> str:
    normalized_raw = normalize_category_value(raw_category)
    if normalized_raw:
        return normalized_raw

    combined = " ".join([normalize_text(title), normalize_text(statement), normalize_text(cause)]).lower()

    if any(term in combined for term in ["compliance", "regulation", "labour law", "employment act", "legal"]):
        return "Compliance/Legal"
    if any(term in combined for term in ["staff", "talent", "retention", "morale", "employee", "recruit", "succession", "turnover", "hr"]):
        return "People/HR"
    if any(term in combined for term in ["budget", "funding", "financial", "revenue", "cash"]):
        return "Financial"
    if any(term in combined for term in ["system", "cyber", "data", "technology", "it", "ict", "information technology"]):
        return "ICT/Cyber"
    if any(term in combined for term in ["safety", "injury", "accident", "health"]):
        return "Health/Safety"
    if any(term in combined for term in ["strategy", "strategic"]):
        return "Strategic"
    if any(term in combined for term in ["reputation", "brand", "image"]):
        return "Reputational"
    if any(term in combined for term in ["environment", "climate", "pollution"]):
        return "Environmental"
    return "Operational"

# =============================================================================
# SHEET / HEADER DISCOVERY (unchanged)
# =============================================================================
FIELD_PATTERNS: Dict[str, List[str]] = {
    "risk_no": [r"risk\s*no", r"risk\s*id", r"identifier"],
    "objective": [r"link\s*to\s*objective", r"objective"],
    "risk_name": [r"risk\s*description", r"risk\s*title", r"risk\s*name", r"name\s*of\s*risk", r"^risk$", r"key\s*risk"],
    "risk_statement": [r"risk\s*definition", r"statement", r"risk\s*event", r"risk\s*detail", r"risk\s*narrative", r"description\s*of\s*risk"],
    "cause": [r"cause", r"root\s*cause"],
    "category": [r"risk\s*category", r"category", r"type\s*of\s*risk"],
    "impact_text": [r"impact", r"severity"],
    "likelihood_text": [r"likelihood", r"probability"],
    "controls": [r"controls", r"control\s*measure", r"existing\s*controls", r"current\s*controls"],
    "control_effectiveness_text": [r"control\s*effectiveness", r"effectiveness"],
    "owner": [r"risk\s*owner", r"owner", r"accountable", r"action\s*owner"],
    "strategy": [r"risk\s*strategy", r"strategy", r"response"],
    "treatment": [r"risk\s*treatment", r"action\s*plan", r"mitigation", r"treatment", r"management\s*action"],
    "status": [r"status", r"progress", r"quarter\s*status", r"update\s*status"],
    "due_date": [r"treatment\s*due\s*date", r"due\s*date", r"target\s*date", r"completion\s*date"],
}

def is_helper_sheet(sheet_name: str) -> bool:
    s = sheet_name.lower()
    helper_keywords = [
        "boundary", "impact", "likelihood", "effectiveness", "matrix", "lookup",
        "legend", "instruction", "guide", "dashboard", "heatmap", "chart", "pivot",
    ]
    return any(word in s for word in helper_keywords)

def detect_header_row_and_columns(ws, scan_rows: int = 30, max_cols: int = 50) -> Tuple[Optional[int], Dict[str, int], int]:
    best_row = None
    best_map: Dict[str, int] = {}
    best_score = 0

    for row_idx in range(1, min(scan_rows, ws.max_row) + 1):
        current_map: Dict[str, int] = {}
        score = 0

        for col_idx in range(1, min(max_cols, ws.max_column) + 1):
            val = normalize_text(merged_cell_value(ws, row_idx, col_idx)).lower()
            if not val:
                continue

            for field, patterns in FIELD_PATTERNS.items():
                if field in current_map:
                    continue
                if any(re.search(pattern, val) for pattern in patterns):
                    current_map[field] = col_idx
                    score += 1
                    break

        if score > best_score and ("risk_name" in current_map or "risk_statement" in current_map):
            best_row = row_idx
            best_map = current_map
            best_score = score

    return best_row, best_map, best_score

def rank_candidate_sheets(wb) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        header_row, col_map, score = detect_header_row_and_columns(ws)

        name_bonus = 0
        lower_name = sheet_name.lower()
        if "risk" in lower_name:
            name_bonus += 3
        if "register" in lower_name:
            name_bonus += 3
        if "monitor" in lower_name:
            name_bonus += 2
        if "quarter" in lower_name:
            name_bonus += 2
        if is_helper_sheet(sheet_name):
            name_bonus -= 4

        total_score = score + name_bonus

        candidates.append({
            "sheet_name": sheet_name,
            "header_row": header_row,
            "column_map": col_map,
            "header_score": score,
            "total_score": total_score,
        })

    candidates.sort(key=lambda x: x["total_score"], reverse=True)
    return candidates

# =============================================================================
# ROW ACCEPTANCE SCORING (unchanged)
# =============================================================================
def is_valid_risk_record(row: Dict[str, Any]) -> Tuple[bool, int, str]:
    evidence = 0
    reasons = []

    if row.get("risk_name") and len(row["risk_name"]) > 3:
        evidence += 3
        reasons.append("has_risk_name")
    else:
        reasons.append("missing_risk_name")
        return False, evidence, " | ".join(reasons)

    if row.get("risk_statement") and len(row["risk_statement"]) > 20:
        evidence += 3
        reasons.append("has_risk_statement")
    else:
        reasons.append("missing_or_short_statement")
        return False, evidence, " | ".join(reasons)

    if row.get("cause"):
        evidence += 1
        reasons.append("has_cause")
    if row.get("controls"):
        evidence += 1
        reasons.append("has_controls")
    if row.get("owner") and row["owner"].lower() != "not assigned":
        evidence += 1
        reasons.append("has_owner")
    if row.get("category"):
        evidence += 1
        reasons.append("has_category")
    if row.get("impact_score") is not None:
        evidence += 1
        reasons.append("has_impact")
    if row.get("likelihood_score") is not None:
        evidence += 1
        reasons.append("has_likelihood")
    if row.get("treatment_plan"):
        evidence += 1
        reasons.append("has_treatment")

    is_valid = evidence >= 6
    reason_str = " | ".join(reasons)
    return is_valid, evidence, reason_str

def looks_like_continuation_fragment(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    if re.match(r"^\d+\.\s+", t):
        return True
    if t.startswith("- ") or t.startswith("• "):
        return True
    if t and t[0].islower():
        return True
    return False

def merge_continuation_rows(rows: List[Dict]) -> List[Dict]:
    merged = []
    current = None
    for row in rows:
        stmt = row.get("risk_statement", "")
        if looks_like_continuation_fragment(stmt) and not row.get("risk_name"):
            if current:
                current["risk_statement"] += " " + stmt
            continue

        if row.get("risk_name") or row.get("risk_statement"):
            if current:
                merged.append(current)
            current = row.copy()
        else:
            if current:
                current["risk_statement"] += " " + (row.get("risk_statement") or "")
                current["cause"] += " " + (row.get("cause") or "")
                current["controls"] += " " + (row.get("controls") or "")
                current["treatment_plan"] += " " + (row.get("treatment_plan") or "")
    if current:
        merged.append(current)
    return merged

def parse_risk_score(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    s = normalize_text(val).lower()
    if s in {"#n/a", "#value!", "#ref!", "#name?", "#num!", "#null!", "none", "nan", ""}:
        return None

    impact_map = {"critical": 5, "major": 4, "moderate": 3, "significant": 2, "minor": 1}
    likelihood_map = {"almost certain": 5, "likely": 4, "moderate": 3, "unlikely": 2, "rare": 1}

    if s in impact_map:
        return float(impact_map[s])
    if s in likelihood_map:
        return float(likelihood_map[s])

    match = re.search(r"(\d+(?:\.\d+)?)", s)
    if match:
        num = float(match.group(1))
        if 1 <= num <= 5:
            return num
    return None

def parse_control_effectiveness(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    s = normalize_text(val).lower()
    mapping = {
        "very good": 0.8,
        "good": 0.6,
        "satisfactory": 0.4,
        "moderate": 0.4,
        "weak": 0.2,
        "unsatisfactory": 0.1,
        "ineffective": 0.1,
    }
    for key, num in mapping.items():
        if key in s:
            return num

    match = re.search(r"(\d+(?:\.\d+)?)", s)
    if match:
        num = float(match.group(1))
        if 0 <= num <= 1:
            return num
        elif 1 < num <= 5:
            return (num - 1) / 4.0
    return None

def compute_scores(
    raw_impact: Any,
    raw_likelihood: Any,
    raw_control_effectiveness: Any
) -> Tuple[int, int, float, float, Optional[float], str]:
    impact_score = parse_risk_score(raw_impact)
    likelihood_score = parse_risk_score(raw_likelihood)

    # Default to 3 if missing
    if impact_score is None:
        impact_score = 3.0
    if likelihood_score is None:
        likelihood_score = 3.0

    control_eff_factor = parse_control_effectiveness(raw_control_effectiveness)
    control_eff_text = normalize_text(raw_control_effectiveness) or "Not rated"

    # INHERENT = IMPACT × LIKELIHOOD
    inherent = round(impact_score * likelihood_score)
    inherent = min(25, max(1, inherent))

    # RESIDUAL = INHERENT × (1 − CONTROL EFFECTIVENESS)
    if control_eff_factor is not None:
        residual = round(inherent * (1.0 - control_eff_factor))
    else:
        residual = inherent

    residual = min(25, max(1, residual))

    return residual, inherent, impact_score, likelihood_score, control_eff_factor, control_eff_text

def parse_structured_sheet(
    ws,
    sheet_name: str,
    col_map: Dict[str, int],
    header_row: int,
    file_name: str,
    default_residual: int
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    raw_rows: List[Dict[str, Any]] = []
    row_audit: List[Dict[str, Any]] = []

    explicit = detect_explicit_division(ws, header_row=header_row)
    division_name, division_source, division_conf = get_division_for_risk(file_name, sheet_name, explicit)

    def get_field(row_idx: int, field_name: str) -> Any:
        col_idx = col_map.get(field_name)
        if not col_idx:
            return None
        return merged_cell_value(ws, row_idx, col_idx)

    blank_streak = 0
    start_row = header_row + 1

    for row_idx in range(start_row, min(ws.max_row + 1, start_row + 300)):
        raw_risk_no = get_field(row_idx, "risk_no")
        raw_objective = get_field(row_idx, "objective")
        raw_risk_name = get_field(row_idx, "risk_name")
        raw_risk_statement = get_field(row_idx, "risk_statement")
        raw_cause = get_field(row_idx, "cause")
        raw_category = get_field(row_idx, "category")
        raw_impact_text = get_field(row_idx, "impact_text")
        raw_likelihood_text = get_field(row_idx, "likelihood_text")
        raw_controls = get_field(row_idx, "controls")
        raw_control_eff = get_field(row_idx, "control_effectiveness_text")
        raw_owner = get_field(row_idx, "owner")
        raw_strategy = get_field(row_idx, "strategy")
        raw_treatment = get_field(row_idx, "treatment")
        raw_status = get_field(row_idx, "status")
        raw_due_date = get_field(row_idx, "due_date")

        risk_no = normalize_text(raw_risk_no)
        objective = normalize_text(raw_objective)
        risk_name = normalize_text(raw_risk_name)
        risk_statement = normalize_text(raw_risk_statement)
        cause = normalize_text(raw_cause)
        category_raw = normalize_text(raw_category)
        controls = normalize_text(raw_controls)
        owner = normalize_text(raw_owner)
        strategy = normalize_text(raw_strategy) or "Treat"
        treatment = normalize_text(raw_treatment)
        status = normalize_text(raw_status) or "Active"
        due_date_raw = normalize_text(raw_due_date)

        combined_gate = " ".join([risk_no, risk_name, risk_statement, cause, controls]).strip()
        if not combined_gate:
            blank_streak += 1
            if blank_streak >= 12:
                break
            continue
        blank_streak = 0

        lower_gate = combined_gate.lower()
        metadata_indicators = [
            "risk description", "risk definition", "link to objective", "control effectiveness",
            "treatment due date", "risk strategy", "risk owner", "impact (1-5)", "likelihood score",
            "inherent risk", "residual risk", "division/dept", "what is in place to prevent",
        ]
        if any(ind in lower_gate for ind in metadata_indicators):
            continue

        if risk_name.startswith("=") or risk_statement.startswith("="):
            continue

        residual, inherent, impact_score, likelihood_score, control_eff_factor, control_eff_text = compute_scores(
            raw_impact=raw_impact_text,
            raw_likelihood=raw_likelihood_text,
            raw_control_effectiveness=raw_control_eff
        )

        parsed_due_date = parse_due_date(raw_due_date)
        category = infer_category_from_text(
            title=risk_name,
            statement=risk_statement,
            cause=cause,
            raw_category=category_raw,
        )

        raw_rows.append({
            "division": division_name,
            "division_source": division_source,
            "division_confidence": division_conf,
            "risk_no": risk_no,
            "objective_link": objective,
            "risk_name": risk_name,
            "risk_statement": risk_statement,
            "cause": cause,
            "category": category,
            "inherent_score": inherent,
            "residual_score": residual,
            "impact_score": impact_score,
            "likelihood_score": likelihood_score,
            "owner": owner or "Not assigned",
            "status": status,
            "due_date": parsed_due_date,
            "due_date_raw": due_date_raw,
            "controls": controls,
            "control_effectiveness": control_eff_text,
            "control_effectiveness_factor": control_eff_factor,
            "strategy": strategy,
            "treatment_plan": treatment,
            "source_file": file_name,
            "source_sheet": sheet_name,
            "source_row": row_idx,
        })

    raw_rows = merge_continuation_rows(raw_rows)

    accepted_risks = []
    for row in raw_rows:
        is_valid, confidence, reason = is_valid_risk_record(row)
        if is_valid:
            row["acceptance_score"] = confidence
            row["acceptance_reason"] = reason
            row["parser_confidence"] = 0.95
            accepted_risks.append(row)
            row_audit.append({
                "row": row["source_row"],
                "risk_name": row["risk_name"],
                "status": "accepted",
                "confidence": confidence,
                "reason": reason,
            })
        else:
            row_audit.append({
                "row": row["source_row"],
                "risk_name": row["risk_name"],
                "status": "rejected",
                "confidence": confidence,
                "reason": reason,
            })

    debug = {
        "sheet_used": sheet_name,
        "header_row": header_row,
        "column_map": col_map,
        "rows_scanned": len(raw_rows) + len(row_audit) - len(accepted_risks),
        "rows_accepted": len(accepted_risks),
        "acceptance_rate": round(len(accepted_risks) / max(1, len(raw_rows)) * 100, 1),
        "row_audit_preview": row_audit[:100],
    }

    return pd.DataFrame(accepted_risks), debug

# =============================================================================
# UNIVERSAL PARSER ENTRY POINT (unchanged)
# =============================================================================
def parse_structured_risk_register(
    file_bytes: bytes,
    file_name: str,
    default_residual: int
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    debug_info: Dict[str, Any] = {
        "parser": "intelligent_structured_v14",
        "candidate_sheets": [],
        "selected_sheets": [],
        "error": None,
    }

    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True)

        candidates = rank_candidate_sheets(wb)
        debug_info["candidate_sheets"] = candidates[:10]

        strong_candidates = [
            c for c in candidates
            if c["header_row"] is not None
            and c["header_score"] >= 5
            and "risk_name" in c["column_map"]
            and "risk_statement" in c["column_map"]
            and "impact_text" in c["column_map"]
            and "likelihood_text" in c["column_map"]
        ]

        if not strong_candidates:
            strong_candidates = [
                c for c in candidates
                if c["header_row"] is not None
                and c["header_score"] >= 4
                and "risk_name" in c["column_map"]
                and ("risk_statement" in c["column_map"] or "cause" in c["column_map"])
            ]

        if not strong_candidates:
            debug_info["error"] = "No structured risk register sheet detected"
            return pd.DataFrame(), debug_info

        best = strong_candidates[0]
        debug_info["selected_sheets"] = [best["sheet_name"]]

        ws = wb[best["sheet_name"]]
        df_sheet, parse_debug = parse_structured_sheet(
            ws=ws,
            sheet_name=best["sheet_name"],
            col_map=best["column_map"],
            header_row=best["header_row"],
            file_name=file_name,
            default_residual=default_residual,
        )
        debug_info.update(parse_debug)

        if not df_sheet.empty:
            return df_sheet, debug_info

        debug_info["error"] = "No valid risks parsed after acceptance scoring"
        return pd.DataFrame(), debug_info

    except Exception as exc:
        debug_info["error"] = str(exc)
        return pd.DataFrame(), debug_info

# =============================================================================
# SIMPLE FALLBACK PARSER (unchanged)
# =============================================================================
def simple_fallback_parser(
    file_bytes: bytes,
    file_name: str,
    default_residual: int
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    risks: List[Dict[str, Any]] = []
    debug_info: Dict[str, Any] = {"parser": "simple_fallback", "cells_scanned": 0, "risks_found": 0}

    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True)

        for sheet_name in wb.sheetnames:
            if is_helper_sheet(sheet_name):
                continue

            ws = wb[sheet_name]

            for row_idx in range(1, min(ws.max_row + 1, 200)):
                for col_idx in range(1, min(ws.max_column + 1, 30)):
                    val = merged_cell_value(ws, row_idx, col_idx)
                    debug_info["cells_scanned"] += 1

                    if isinstance(val, str):
                        text = normalize_text(val)
                        if len(text) > 40 and not text.startswith("="):
                            metadata_indicators = [
                                "risk monitoring", "period of assessment", "department/ unit",
                                "description of control", "overall effectiveness", "control categorisation",
                                "action plan", "residual risk", "risk strategy", "inherent risk",
                                "risk owner", "impact rating", "likelihood rating", "root cause",
                                "consequences", "start date", "target completion", "processed by",
                                "approved by", "recommended by", "signature", "link to objective",
                                "risk no", "division/ dept",
                            ]
                            if not any(ind in text.lower() for ind in metadata_indicators):
                                division_name, division_source, division_conf = get_division_for_risk(file_name, sheet_name, None)
                                impact = 3.0
                                likelihood = 3.0
                                inherent = round(impact * likelihood)
                                row = {
                                    "division": division_name,
                                    "division_source": division_source,
                                    "division_confidence": division_conf,
                                    "risk_no": "",
                                    "objective_link": "",
                                    "risk_name": text[:80],
                                    "risk_statement": text[:500],
                                    "cause": "",
                                    "category": "Uncategorised",
                                    "inherent_score": inherent,
                                    "residual_score": inherent,
                                    "impact_score": impact,
                                    "likelihood_score": likelihood,
                                    "owner": "Not assigned",
                                    "status": "Active",
                                    "due_date": None,
                                    "due_date_raw": "",
                                    "controls": "",
                                    "control_effectiveness": "Not rated",
                                    "control_effectiveness_factor": None,
                                    "strategy": "Treat",
                                    "treatment_plan": "",
                                    "source_file": file_name,
                                    "source_sheet": sheet_name,
                                    "source_row": row_idx,
                                }
                                is_valid, conf, reason = is_valid_risk_record(row)
                                if is_valid:
                                    row["acceptance_score"] = conf
                                    row["acceptance_reason"] = reason
                                    row["parser_confidence"] = 0.60
                                    risks.append(row)
                                    debug_info["risks_found"] += 1
                                    if len(risks) >= 20:
                                        break
                if len(risks) >= 20:
                    break
            if len(risks) >= 20:
                break

    except Exception as exc:
        debug_info["error"] = str(exc)

    if risks:
        return pd.DataFrame(risks), debug_info
    return pd.DataFrame(), debug_info

# =============================================================================
# FINAL DISPATCHER (unchanged)
# =============================================================================
def parse_uploaded_file_bytes(
    file_bytes: bytes,
    file_name: str,
    default_residual: int
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df, debug = parse_structured_risk_register(file_bytes, file_name, default_residual)
    if not df.empty:
        st.success(f"✅ Extracted {len(df)} risks (acceptance rate {debug.get('acceptance_rate', 0)}%)")
        return df, debug

    df, fallback_debug = simple_fallback_parser(file_bytes, file_name, default_residual)
    if not df.empty:
        st.warning(f"⚠️ Simple fallback extracted {len(df)} potential risks")
        return df, fallback_debug

    return pd.DataFrame(), debug

# =============================================================================
# ENTERPRISE REGISTER BUILDING (unchanged)
# =============================================================================
def build_enterprise_register(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if raw_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    def safe_col(col_name, default_val=None):
        if col_name in raw_df.columns:
            return raw_df[col_name].fillna(default_val).tolist()
        else:
            return [default_val] * len(raw_df)

    statements = safe_col("risk_statement", "")
    names = safe_col("risk_name", "")
    divisions = safe_col("division", "Unknown Division")
    categories = safe_col("category", "Uncategorised")
    inherent_scores = safe_col("inherent_score", 0)
    residual_scores = safe_col("residual_score", 0)
    impact_scores = safe_col("impact_score", 3.0)
    likelihood_scores = safe_col("likelihood_score", 3.0)
    owners = safe_col("owner", "Not assigned")
    strategies = safe_col("strategy", "Treat")
    treatments = safe_col("treatment_plan", "")
    statuses = safe_col("status", "Active")
    due_dates = safe_col("due_date", None)
    controls_list = safe_col("controls", "")
    control_eff_texts = safe_col("control_effectiveness", "Not rated")
    control_eff_factors = safe_col("control_effectiveness_factor", None)
    acceptance_scores = safe_col("acceptance_score", 0)
    parser_confidences = safe_col("parser_confidence", 0.6)
    division_confidences = safe_col("division_confidence", 0.5)
    source_files = safe_col("source_file", "")
    source_sheets = safe_col("source_sheet", "")
    source_rows = safe_col("source_row", 0)

    clusters = []
    used = set()

    for i, stmt in enumerate(statements):
        if i in used:
            continue
        matches = process.extract(stmt, statements, scorer=fuzz.token_sort_ratio, limit=10)
        cluster_indices = [i]
        similarities = [100.0]
        for match in matches:
            if match[1] >= 70 and match[2] not in used:
                cluster_indices.append(match[2])
                similarities.append(match[1])
                used.add(match[2])
        used.update(cluster_indices)
        clusters.append((cluster_indices, similarities))

    enterprise_rows = []
    clusters_detail = []

    for cluster_id, (indices, similarities) in enumerate(clusters):
        cluster_names = [names[idx] for idx in indices]
        cluster_stmts = [statements[idx] for idx in indices]
        cluster_divisions = [divisions[idx] for idx in indices]
        cluster_categories = [categories[idx] for idx in indices]
        cluster_inherents = [inherent_scores[idx] for idx in indices]
        cluster_residuals = [residual_scores[idx] for idx in indices]
        cluster_impacts = [impact_scores[idx] for idx in indices]
        cluster_likelihoods = [likelihood_scores[idx] for idx in indices]
        cluster_owners = [owners[idx] for idx in indices]
        cluster_strategies = [strategies[idx] for idx in indices]
        cluster_treatments = [treatments[idx] for idx in indices]
        cluster_statuses = [statuses[idx] for idx in indices]
        cluster_due_dates = [due_dates[idx] for idx in indices]
        cluster_controls = [controls_list[idx] for idx in indices]
        cluster_control_eff_text = [control_eff_texts[idx] for idx in indices]
        cluster_control_eff_factor = [control_eff_factors[idx] for idx in indices if control_eff_factors[idx] is not None]
        cluster_acceptance = [acceptance_scores[idx] for idx in indices]
        cluster_parser_conf = [parser_confidences[idx] for idx in indices]
        cluster_div_conf = [division_confidences[idx] for idx in indices]
        cluster_sources = [
            f"{source_files[idx]}:{source_sheets[idx]}:row{source_rows[idx]}"
            for idx in indices
        ]

        avg_similarity = sum(similarities) / len(similarities) if similarities else 0
        acceptance_avg = sum(cluster_acceptance) / len(cluster_acceptance) if cluster_acceptance else 0
        parser_conf_avg = sum(cluster_parser_conf) / len(cluster_parser_conf) if cluster_parser_conf else 0
        div_conf_avg = sum(cluster_div_conf) / len(cluster_div_conf) if cluster_div_conf else 0

        unique_divs = set(cluster_divisions)
        div_consistency = 1.0 if len(unique_divs) == 1 else 0.6 if len(unique_divs) <= 2 else 0.3
        unique_cats = set(cluster_categories)
        cat_consistency = 1.0 if len(unique_cats) == 1 else 0.7 if len(unique_cats) <= 2 else 0.4

        cluster_confidence = (
            (avg_similarity / 100) * 0.30 +
            (acceptance_avg / 10) * 0.20 +
            (parser_conf_avg) * 0.15 +
            (div_conf_avg) * 0.10 +
            (len(indices) / 5) * 0.10 +
            div_consistency * 0.10 +
            cat_consistency * 0.05
        ) * 100
        cluster_confidence = round(min(100, cluster_confidence))

        canonical_name = max(cluster_names, key=len) if cluster_names else "Unnamed Risk"
        canonical_stmt = max(cluster_stmts, key=len) if cluster_stmts else ""
        div_counts = pd.Series(cluster_divisions).value_counts()
        primary_division = div_counts.index[0] if not div_counts.empty else "Unknown Division"
        cat_counts = pd.Series(cluster_categories).value_counts()
        primary_category = cat_counts.index[0] if not cat_counts.empty else "Uncategorised"
        max_residual = max(cluster_residuals) if cluster_residuals else 0
        max_inherent = max(cluster_inherents) if cluster_inherents else 0
        avg_impact = round(sum(cluster_impacts) / len(cluster_impacts), 1) if cluster_impacts else 0
        avg_likelihood = round(sum(cluster_likelihoods) / len(cluster_likelihoods), 1) if cluster_likelihoods else 0
        all_owners = ", ".join(sorted(set(o for o in cluster_owners if o.lower() != "not assigned"))) or "Not assigned"
        primary_owner = cluster_owners[0] if cluster_owners else "Not assigned"
        all_divisions_str = ", ".join(sorted(unique_divs))

        primary_controls = max([c for c in cluster_controls if c], key=len) if any(cluster_controls) else ""
        all_controls = " | ".join(sorted(set(c for c in cluster_controls if c)))
        avg_control_eff = round(sum(cluster_control_eff_factor) / len(cluster_control_eff_factor), 2) if cluster_control_eff_factor else None
        worst_control_eff = max(cluster_control_eff_factor) if cluster_control_eff_factor else None
        worst_control_text = cluster_control_eff_text[cluster_control_eff_factor.index(worst_control_eff)] if worst_control_eff in cluster_control_eff_factor else "Not rated"

        mitigation_strength = round((max_inherent - max_residual) / max_inherent * 100, 1) if max_inherent > 0 else 0.0

        strategy_counts = pd.Series([s for s in cluster_strategies if s]).value_counts()
        primary_strategy = strategy_counts.index[0] if not strategy_counts.empty else "Treat"
        all_strategies = ", ".join(sorted(set(s for s in cluster_strategies if s)))
        primary_treatment = max([t for t in cluster_treatments if t], key=len) if any(cluster_treatments) else ""
        all_treatments = " | ".join(sorted(set(t for t in cluster_treatments if t)))
        valid_dates = [d for d in cluster_due_dates if d is not None]
        earliest_due = min(valid_dates) if valid_dates else None
        latest_due = max(valid_dates) if valid_dates else None
        status_counts = pd.Series([s for s in cluster_statuses if s]).value_counts()
        if "Overdue" in status_counts or "Delayed" in status_counts:
            treatment_status = "Overdue"
        elif "Completed" in status_counts or "Closed" in status_counts:
            treatment_status = "Completed"
        elif "On Track" in status_counts or "Active" in status_counts:
            treatment_status = "On Track"
        else:
            treatment_status = "Mixed"

        enterprise_rows.append({
            "enterprise_risk_id": f"ER-{cluster_id+1:03d}",
            "risk_name": canonical_name,
            "risk_statement": canonical_stmt,
            "primary_division": primary_division,
            "all_contributing_divisions": all_divisions_str,
            "primary_category": primary_category,
            "all_categories": ", ".join(sorted(unique_cats)),
            "inherent_score": max_inherent,
            "residual_score": max_residual,
            "impact_pre": avg_impact,
            "likelihood_pre": avg_likelihood,
            "primary_owner": primary_owner,
            "all_owners": all_owners,
            "primary_strategy": primary_strategy,
            "all_strategies": all_strategies,
            "primary_treatment_plan": primary_treatment,
            "all_treatment_plans": all_treatments,
            "earliest_due_date": earliest_due,
            "latest_due_date": latest_due,
            "treatment_status": treatment_status,
            "primary_controls": primary_controls,
            "all_controls": all_controls,
            "control_effectiveness": worst_control_text,
            "control_effectiveness_factor": worst_control_eff,
            "avg_control_effectiveness": avg_control_eff,
            "mitigation_strength_pct": mitigation_strength,
            "cluster_size": len(indices),
            "cluster_confidence": cluster_confidence,
            "source_risks": cluster_names,
            "source_lineage": " | ".join(cluster_sources[:3]) + ("..." if len(cluster_sources) > 3 else ""),
        })

        clusters_detail.append({
            "cluster_id": f"ER-{cluster_id+1:03d}",
            "cluster_size": len(indices),
            "avg_similarity": round(avg_similarity, 1),
            "cluster_confidence": cluster_confidence,
            "primary_division": primary_division,
            "all_divisions": all_divisions_str,
            "primary_category": primary_category,
            "mitigation_strength": mitigation_strength,
            "canonical_name": canonical_name,
        })

    enterprise_df = pd.DataFrame(enterprise_rows)
    if not enterprise_df.empty:
        enterprise_df["residual_level"] = enterprise_df["residual_score"].apply(
            lambda x: "Critical" if x >= 20 else "High" if x >= 12 else "Medium" if x >= 6 else "Low"
        )
    clusters_detail_df = pd.DataFrame(clusters_detail)
    return enterprise_df, clusters_detail_df

def parse_all_files(uploaded_files, tier: str, default_residual: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[Dict]]:
    all_raw = []
    all_debug = []
    total_scanned = 0
    total_accepted = 0

    for file in uploaded_files:
        df, debug = cached_parse_file(file.getvalue(), file.name, default_residual)
        all_debug.append(debug)
        total_scanned += debug.get("rows_scanned", 0)
        total_accepted += len(df)
        if not df.empty:
            all_raw.append(df)

    if not all_raw:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), all_debug

    raw_df = pd.concat(all_raw, ignore_index=True)
    raw_df["_stmt_norm"] = raw_df["risk_statement"].fillna("").apply(lambda x: re.sub(r"[^\w\s]", "", x.lower()).strip())
    raw_df = raw_df.drop_duplicates(subset=["_stmt_norm"], keep="first")
    raw_df = raw_df.drop(columns=["_stmt_norm"])

    enterprise_df, clusters_detail_df = build_enterprise_register(raw_df)

    if tier == "free":
        raw_df = raw_df.head(10)
        enterprise_df = enterprise_df.head(10)
        clusters_detail_df = clusters_detail_df.head(10)

    st.session_state.parser_audit = {
        "total_files": len(uploaded_files),
        "raw_risks": len(raw_df),
        "enterprise_risks": len(enterprise_df),
        "rows_scanned": total_scanned,
        "rows_accepted": total_accepted,
        "acceptance_rate": round(total_accepted / max(1, total_scanned) * 100, 1),
        "clusters_formed": len(enterprise_df),
        "low_confidence_clusters": len(clusters_detail_df[clusters_detail_df["cluster_confidence"] < 60]) if not clusters_detail_df.empty else 0,
    }

    return raw_df, enterprise_df, clusters_detail_df, all_debug

# =============================================================================
# INTELLIGENCE ENGINE (UPDATED HEALTH SCORE)
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
    """
    Calculate a realistic enterprise health score based on configurable weights.
    Uses global HEALTH_WEIGHTS dictionary.
    """
    if df.empty:
        return 0.0

    # 1. Residual exposure burden (weighted)
    critical_high_ratio = (df["residual_level"].isin(["Critical", "High"])).mean()
    # Scale: 0% critical/high -> 100, 100% -> 0
    exposure_score = (1 - critical_high_ratio) * 100

    # 2. Control effectiveness (weighted)
    if "avg_control_effectiveness" in df.columns and df["avg_control_effectiveness"].notna().any():
        avg_control = df["avg_control_effectiveness"].mean()
    else:
        avg_control = df["mitigation_strength_pct"].mean() / 100
    control_score = avg_control * 100

    # 3. Treatment confidence (weighted)
    if "treatment_status" in df.columns:
        on_track_ratio = (df["treatment_status"] == "On Track").mean()
        completed_ratio = (df["treatment_status"] == "Completed").mean()
        treatment_score = (on_track_ratio * 70 + completed_ratio * 100)
    else:
        treatment_score = 50

    # 4. Appetite compliance (weighted)
    if "appetite_band" in df.columns:
        within_ratio = (df["appetite_band"] == "within appetite").mean()
        near_ratio = (df["appetite_band"] == "near appetite").mean()
        appetite_score = (within_ratio * 100 + near_ratio * 50)
    else:
        appetite_score = 50

    # 5. Concentration penalty (reduces final score)
    div_exposure = df.groupby("primary_division")["residual_score"].sum()
    if not div_exposure.empty:
        top_div_pct = div_exposure.max() / div_exposure.sum()
        if top_div_pct > CONCENTRATION_PENALTY_THRESHOLD:
            penalty = min(MAX_CONCENTRATION_PENALTY, (top_div_pct - CONCENTRATION_PENALTY_THRESHOLD) * 100)
        else:
            penalty = 0
    else:
        penalty = 0

    # Weighted sum, then subtract penalty
    final_score = (
        exposure_score * HEALTH_WEIGHTS["exposure"] +
        control_score * HEALTH_WEIGHTS["control"] +
        treatment_score * HEALTH_WEIGHTS["treatment"] +
        appetite_score * HEALTH_WEIGHTS["appetite"] -
        penalty * HEALTH_WEIGHTS["concentration_penalty"]
    )
    return max(0.0, round(final_score, 1))

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

def build_intelligence_snapshot(enterprise_df: pd.DataFrame, threshold: int, category_appetite: Dict = None) -> Dict[str, Any]:
    if enterprise_df.empty:
        return {}
    snapshot = {}
    snapshot["critical_count"] = int((enterprise_df["residual_level"] == "Critical").sum())
    snapshot["high_count"] = int((enterprise_df["residual_level"] == "High").sum())
    snapshot["avg_residual"] = round(enterprise_df["residual_score"].mean(), 1)
    snapshot["avg_inherent"] = round(enterprise_df["inherent_score"].mean(), 1)
    snapshot["avg_mitigation_strength"] = round(enterprise_df["mitigation_strength_pct"].mean(), 1)
    exposure_by_div = enterprise_df.groupby("primary_division")["residual_score"].sum().sort_values(ascending=False)
    if not exposure_by_div.empty:
        snapshot["top_division"] = exposure_by_div.index[0]
        snapshot["top_division_pct"] = round((exposure_by_div.iloc[0] / exposure_by_div.sum()) * 100, 1)
        snapshot["division_exposure"] = exposure_by_div.head(5).to_dict()
    else:
        snapshot["top_division"] = "N/A"
        snapshot["top_division_pct"] = 0
        snapshot["division_exposure"] = {}
    exposure_by_cat = enterprise_df.groupby("primary_category")["residual_score"].sum().sort_values(ascending=False)
    snapshot["category_exposure"] = exposure_by_cat.head(5).to_dict()
    enterprise_df["appetite_band"] = enterprise_df.apply(
        lambda row: appetite_band(row["residual_score"], threshold, row.get("primary_category", ""), category_appetite), axis=1
    )
    snapshot["pct_within_appetite"] = round((enterprise_df["appetite_band"] == "within appetite").mean() * 100, 1)
    snapshot["pct_near_appetite"] = round((enterprise_df["appetite_band"] == "near appetite").mean() * 100, 1)
    snapshot["pct_breached"] = round((enterprise_df["appetite_band"].isin(["breached", "critical breach"])).mean() * 100, 1)
    snapshot["emerging_themes"] = detect_emerging_themes(enterprise_df)
    snapshot["enterprise_health_score"] = calculate_enterprise_health_score(enterprise_df)

    conf_score = 0.0
    if "primary_owner" in enterprise_df.columns:
        conf_score += (enterprise_df["primary_owner"] != "Not assigned").mean() * 30
    if "treatment_status" in enterprise_df.columns:
        conf_score += (enterprise_df["treatment_status"] == "On Track").mean() * 30
        conf_score += (enterprise_df["treatment_status"] == "Completed").mean() * 20
    if "earliest_due_date" in enterprise_df.columns:
        today = date.today()
        future_due = enterprise_df["earliest_due_date"].apply(lambda d: d > today if pd.notna(d) else False).mean()
        conf_score += future_due * 20
    if "avg_control_effectiveness" in enterprise_df.columns:
        avg_ctrl = enterprise_df["avg_control_effectiveness"].mean()
        if pd.notna(avg_ctrl):
            conf_score += avg_ctrl * 30
    snapshot["treatment_confidence"] = round(min(100, conf_score))

    snapshot["total_risks"] = len(enterprise_df)
    snapshot["board_risks"] = enterprise_df.nlargest(5, "residual_score")[
        ["risk_name", "primary_division", "residual_score", "primary_owner", "primary_category",
         "treatment_status", "earliest_due_date", "mitigation_strength_pct", "cluster_confidence"]
    ].rename(columns={"primary_division": "division", "primary_owner": "owner", "primary_category": "category"}).to_dict("records")
    snapshot["threshold"] = threshold
    return snapshot

def generate_board_narrative(snapshot: Dict, threshold: int, company: str, report_title: str) -> str:
    narrative = []
    narrative.append(f"# {report_title}")
    narrative.append(f"**Organization:** {company}")
    narrative.append(f"**Date:** {datetime.now().strftime('%B %d, %Y')}")
    narrative.append(f"**Reporting Period:** Q{((datetime.now().month-1)//3)+1} {datetime.now().year}")
    narrative.append("")
    health = snapshot.get("enterprise_health_score", 0)
    # Get posture from thresholds
    posture = "Unknown"
    for score_threshold, label in POSTURE_THRESHOLDS:
        if health >= score_threshold:
            posture = label
            break
    narrative.append(f"## 1. Executive Posture Summary")
    narrative.append(f"**Enterprise Health Score:** {health}/100 ({posture})")
    narrative.append(f"**Total Enterprise Risks:** {snapshot.get('critical_count', 0) + snapshot.get('high_count', 0)} critical/high, {snapshot.get('avg_residual', 0):.1f}/25 average residual")
    narrative.append(f"**Average Mitigation Strength:** {snapshot.get('avg_mitigation_strength', 0)}%")
    narrative.append("")
    narrative.append(f"## 2. Concentration Risk Areas")
    narrative.append(f"**Top Division Exposure:** {snapshot.get('top_division', 'N/A')} ({snapshot.get('top_division_pct', 0)}% of enterprise load)")
    narrative.append("")
    narrative.append(f"## 3. Risk Appetite Status (Threshold: {threshold}/25)")
    narrative.append(f"- Within appetite: {snapshot.get('pct_within_appetite', 0)}%")
    narrative.append(f"- Near appetite: {snapshot.get('pct_near_appetite', 0)}%")
    narrative.append(f"- Breached: {snapshot.get('pct_breached', 0)}%")
    narrative.append("")
    narrative.append(f"## 4. Treatment Delivery Confidence")
    narrative.append(f"**Confidence Score:** {snapshot.get('treatment_confidence', 0)}%")
    narrative.append("")
    if snapshot.get("board_risks"):
        narrative.append(f"## 5. Top 5 Board-Attention Risks")
        for risk in snapshot["board_risks"]:
            conf_str = f" (Confidence: {risk.get('cluster_confidence', 'N/A')}%)" if 'cluster_confidence' in risk else ""
            due_str = f" Due: {risk.get('earliest_due_date', 'N/A')}" if risk.get('earliest_due_date') else ""
            status_str = f" Status: {risk.get('treatment_status', 'N/A')}"
            mitigation_str = f" Mitigation: {risk.get('mitigation_strength_pct', 0)}%"
            narrative.append(f"- **{risk['risk_name']}** ({risk['division']}) – Residual: {risk['residual_score']}/25, Owner: {risk['owner']}{due_str}{status_str}{mitigation_str}{conf_str}")
        narrative.append("")
    themes = snapshot.get("emerging_themes", [])
    if themes:
        narrative.append(f"## 6. Emerging Systemic Themes")
        for theme in themes:
            narrative.append(f"- {theme}")
        narrative.append("")
    return "\n".join(narrative)

# =============================================================================
# EXCEL & PDF EXPORTS (unchanged)
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

def generate_intelligent_excel_pack(data: Dict[str, Any], narrative: str) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary = pd.DataFrame({
            "Metric": ["Organization", "Report Title", "Period", "Board Date", "Health Score", "Total Enterprise Risks", "Critical+High", "Avg Residual", "Top Division", "Treatment Confidence", "Avg Mitigation Strength", "Within Appetite", "Near Appetite", "Breached"],
            "Value": [
                data.get("company", ""), data.get("report_title", ""), data.get("period", ""), data.get("board_date", ""),
                data.get("enterprise_health_score", 0), data["total_risks"],
                data.get("critical_count", 0) + data.get("high_count", 0),
                f"{data.get('avg_residual', 0):.1f}/25", f"{data.get('top_division', 'N/A')} ({data.get('top_division_pct', 0)}%)",
                f"{data.get('treatment_confidence', 0)}%", f"{data.get('avg_mitigation_strength', 0)}%",
                f"{data.get('pct_within_appetite', 0)}%", f"{data.get('pct_near_appetite', 0)}%", f"{data.get('pct_breached', 0)}%"
            ]
        })
        summary.to_excel(writer, sheet_name="Executive Summary", index=False)
        pd.DataFrame({"Board Narrative": narrative.split("\n")}).to_excel(writer, sheet_name="Board Narrative", index=False)
        data["enterprise_df"].to_excel(writer, sheet_name="Enterprise Risks", index=False)
        data["raw_df"].to_excel(writer, sheet_name="Raw Accepted Risks", index=False)
        if "clusters_detail_df" in data:
            data["clusters_detail_df"].to_excel(writer, sheet_name="Cluster Review Queue", index=False)
        board_risks = data["enterprise_df"][data["enterprise_df"]["residual_score"] >= data.get("threshold", 12)].copy()
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
        story.append(Paragraph("Top Board-Attention Risks", heading_style))
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
# DASHBOARD CHARTS (unchanged)
# =============================================================================
def create_category_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    cat_exposure = df.groupby("primary_category")["residual_score"].sum().sort_values(ascending=False).head(8)
    fig = go.Figure(data=[go.Bar(x=list(cat_exposure.values), y=list(cat_exposure.index), orientation='h', marker_color='#4A90E2')])
    fig.update_layout(title="Risk Exposure by Category", height=350, plot_bgcolor="white", margin=dict(l=10, r=10, t=40, b=10))
    return fig

def create_division_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure()
    df_plot = df[df["primary_division"] != "Unknown Division"]
    if df_plot.empty:
        return go.Figure()
    div_exposure = df_plot.groupby("primary_division")["residual_score"].sum().sort_values(ascending=False).head(8)
    fig = go.Figure(data=[go.Bar(x=list(div_exposure.index), y=list(div_exposure.values), marker_color='#F97316')])
    fig.update_layout(title="Risk Exposure by Division", height=350, xaxis_tickangle=-30, plot_bgcolor="white", margin=dict(l=10, r=10, t=40, b=80))
    return fig

def create_appetite_gauge(df: pd.DataFrame, threshold: int, category_appetite: Dict = None) -> go.Figure:
    if df.empty:
        return go.Figure()
    df["appetite"] = df.apply(lambda row: appetite_band(row["residual_score"], threshold, row.get("primary_category", ""), category_appetite), axis=1)
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
# UI COMPONENTS (unchanged, but posture uses global thresholds)
# =============================================================================
def apply_custom_theme(primary: str, secondary: str) -> None:
    st.markdown(f"""
    <style>
    body, .stApp {{ background-color: #f9fafb; }}
    .stTabs [data-baseweb="tab-list"] {{ gap: 8px; background-color: transparent; }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 12px 12px 0 0; padding: 12px 24px; background-color: white;
        border: 1px solid #e2e8f0; border-bottom: none; font-weight: 600; color: #475569;
    }}
    .stTabs [aria-selected="true"] {{
        background-color: {primary} !important; color: white !important; border-color: {primary} !important;
    }}
    .stButton > button {{
        background: linear-gradient(145deg, {primary} 0%, {secondary} 100%);
        color: white; border: none; border-radius: 40px; padding: 12px 28px; font-weight: 600;
        transition: all 0.2s; box-shadow: 0 4px 12px rgba(14, 54, 92, 0.2);
    }}
    .stButton > button:hover {{ transform: translateY(-2px); box-shadow: 0 8px 20px rgba(14, 54, 92, 0.3); }}
    .exec-card {{
        background: #ffffff; border-radius: 20px; padding: 22px 24px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04); border: 1px solid rgba(14, 54, 92, 0.08); margin-bottom: 20px;
    }}
    .exec-card-header {{
        display: flex; align-items: center; margin-bottom: 18px; border-bottom: 1px solid #eef2f6; padding-bottom: 14px;
    }}
    .exec-card-title {{ font-size: 1.2rem; font-weight: 700; color: {primary}; letter-spacing: -0.01em; }}
    .exec-badge {{
        display: inline-block; padding: 6px 14px; border-radius: 40px; font-size: 0.8rem;
        font-weight: 700; letter-spacing: 0.2px; text-transform: uppercase;
    }}
    .exec-metric-row {{ display: flex; justify-content: space-between; margin-bottom: 14px; }}
    .exec-metric-label {{ color: #475569; font-size: 0.95rem; }}
    .exec-metric-value {{ font-weight: 700; color: #0f172a; font-size: 1.1rem; }}
    .exec-divider {{ height: 1px; background: #e2e8f0; margin: 18px 0; }}
    .exec-risk-card {{
        background: #f8fafc; border-radius: 16px; padding: 16px 20px; margin-bottom: 12px; border-left: 6px solid;
    }}
    .exec-hero {{
        background: linear-gradient(145deg, {primary} 0%, {secondary} 100%);
        border-radius: 28px; padding: 28px 32px; color: white; margin-bottom: 24px;
        box-shadow: 0 12px 32px rgba(14, 54, 92, 0.2);
    }}
    .kpi-card {{
        background: white; border-radius: 16px; padding: 18px 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.04); border: 1px solid #edf2f7; height: 100%;
    }}
    .kpi-label {{ color: #64748b; font-size: 0.9rem; font-weight: 500; letter-spacing: 0.3px; margin-bottom: 8px; }}
    .kpi-value {{ color: #0f172a; font-size: 2.2rem; font-weight: 700; line-height: 1.2; }}
    .register-table {{
        width: 100%; border-collapse: collapse; font-family: -apple-system, BlinkMacSystemFont, sans-serif; font-size: 13px;
    }}
    .register-table th {{
        background-color: #f3f4f6; color: #1f2937; font-weight: 600; border-bottom: 2px solid #d1d5db;
        padding: 10px 8px; text-align: left; border-right: 1px solid #e5e7eb;
    }}
    .register-table td {{
        padding: 8px; border-right: 1px solid #e5e7eb; border-bottom: 1px solid #e5e7eb; vertical-align: top;
    }}
    .confidence-high {{ background-color: #dcfce7; color: #166534; }}
    .confidence-medium {{ background-color: #fef9c3; color: #854d0e; }}
    .confidence-low {{ background-color: #fee2e2; color: #991b1b; }}
    .status-ontrack {{ background-color: #dcfce7; color: #166534; }}
    .status-overdue {{ background-color: #fee2e2; color: #991b1b; }}
    </style>
    """, unsafe_allow_html=True)

def render_parser_audit_panel():
    if st.session_state.parser_audit:
        with st.expander("🔍 Extraction Quality Dashboard", expanded=False):
            audit = st.session_state.parser_audit
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Files Processed", audit.get("total_files", 0))
            col2.metric("Rows Scanned", audit.get("rows_scanned", 0))
            col3.metric("Rows Accepted", audit.get("rows_accepted", 0))
            col4.metric("Acceptance Rate", f"{audit.get('acceptance_rate', 0)}%")
            col5, col6, col7, col8 = st.columns(4)
            col5.metric("Raw Risks", audit.get("raw_risks", 0))
            col6.metric("Enterprise Risks", audit.get("enterprise_risks", 0))
            col7.metric("Clusters Formed", audit.get("clusters_formed", 0))
            col8.metric("Low Confidence", audit.get("low_confidence_clusters", 0))

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
- Heatmaps  
- Category/Division charts  
""")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("$29/mo", key="pro_monthly"):
                    if not stripe or not STRIPE_SECRET_KEY:
                        st.error("Stripe is not configured.")
                    elif not STRIPE_PRICE_ID_PRO_MONTHLY:
                        st.error("Professional monthly price ID missing.")
                    else:
                        try:
                            session = stripe.checkout.Session.create(
                                payment_method_types=["card"],
                                line_items=[{"price": STRIPE_PRICE_ID_PRO_MONTHLY, "quantity": 1}],
                                mode="subscription",
                                success_url=APP_URL + "?success_pro_monthly=true",
                                cancel_url=APP_URL,
                            )
                            st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"Stripe error: {e}")
            with col2:
                if st.button("$99/yr", key="pro_annual"):
                    if not stripe or not STRIPE_SECRET_KEY:
                        st.error("Stripe is not configured.")
                    elif not STRIPE_PRICE_ID_PRO_ANNUAL:
                        st.error("Professional annual price ID missing.")
                    else:
                        try:
                            session = stripe.checkout.Session.create(
                                payment_method_types=["card"],
                                line_items=[{"price": STRIPE_PRICE_ID_PRO_ANNUAL, "quantity": 1}],
                                mode="subscription",
                                success_url=APP_URL + "?success_pro_annual=true",
                                cancel_url=APP_URL,
                            )
                            st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"Stripe error: {e}")
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
                    if not stripe or not STRIPE_SECRET_KEY:
                        st.error("Stripe is not configured.")
                    elif not STRIPE_PRICE_ID_ENT_MONTHLY:
                        st.error("Enterprise monthly price ID missing.")
                    else:
                        try:
                            session = stripe.checkout.Session.create(
                                payment_method_types=["card"],
                                line_items=[{"price": STRIPE_PRICE_ID_ENT_MONTHLY, "quantity": 1}],
                                mode="subscription",
                                success_url=APP_URL + "?success_ent_monthly=true",
                                cancel_url=APP_URL,
                            )
                            st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"Stripe error: {e}")
            with col2:
                if st.button("$299/yr", key="ent_annual"):
                    if not stripe or not STRIPE_SECRET_KEY:
                        st.error("Stripe is not configured.")
                    elif not STRIPE_PRICE_ID_ENT_ANNUAL:
                        st.error("Enterprise annual price ID missing.")
                    else:
                        try:
                            session = stripe.checkout.Session.create(
                                payment_method_types=["card"],
                                line_items=[{"price": STRIPE_PRICE_ID_ENT_ANNUAL, "quantity": 1}],
                                mode="subscription",
                                success_url=APP_URL + "?success_ent_annual=true",
                                cancel_url=APP_URL,
                            )
                            st.markdown(f"<a href='{session.url}' target='_blank'>Pay</a>", unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"Stripe error: {e}")
            st.markdown("---")
            code = st.text_input("Unlock code", type="password", placeholder="Enter code")
            col_apply, _ = st.columns([1, 2])
            with col_apply:
                apply_btn = st.button("Apply Code", use_container_width=True)

            if apply_btn:
                if code == PRO_UNLOCK_CODE:
                    st.session_state.tier = "professional"
                    st.success("✅ Professional unlocked!")
                    st.rerun()
                elif code == ENT_UNLOCK_CODE:
                    st.session_state.tier = "enterprise"
                    st.success("✅ Enterprise unlocked!")
                    st.rerun()
                elif code:
                    st.error("❌ Invalid unlock code")
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

# =============================================================================
# MAIN APP
# =============================================================================
def main():
    apply_custom_theme(st.session_state.primary_color, st.session_state.secondary_color)
    render_sidebar()

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
                raw_df, enterprise_df, clusters_detail_df, debug_list = parse_all_files(
                    uploaded_files, st.session_state.tier, st.session_state.default_residual_score
                )
                if enterprise_df.empty:
                    st.error("No valid risk data found.")
                    if st.session_state.debug_mode:
                        st.subheader("🔧 Parser Debug Information")
                        for i, debug in enumerate(debug_list):
                            st.markdown(f"**File {i+1}**")
                            st.json(debug)
                else:
                    if (enterprise_df["primary_division"] == "Unknown Division").all():
                        st.warning("⚠️ Division detection failed. Enterprise intelligence may be unreliable until lineage is restored.")
                    st.success(f"✅ {len(enterprise_df)} enterprise risks consolidated from {len(raw_df)} raw accepted risks")
                    if st.session_state.debug_mode:
                        with st.expander("🔧 Enterprise Risks Preview"):
                            st.dataframe(enterprise_df.head(20))

                    category_appetite = st.session_state.category_appetite if st.session_state.tier == "enterprise" else None
                    snapshot = build_intelligence_snapshot(enterprise_df, st.session_state.board_threshold, category_appetite)
                    narrative = generate_board_narrative(snapshot, st.session_state.board_threshold, st.session_state.org_name, st.session_state.report_title)

                    st.session_state.rf_data = {
                        "raw_df": raw_df,
                        "enterprise_df": enterprise_df,
                        "clusters_detail_df": clusters_detail_df,
                        "total_risks": len(enterprise_df),
                        "company": st.session_state.org_name,
                        "report_title": st.session_state.report_title,
                        "period": f"Q{((datetime.now().month-1)//3)+1} {datetime.now().year}",
                        "board_date": datetime.now().strftime("%B %d, %Y"),
                        "threshold": st.session_state.board_threshold,
                        "critical_count": snapshot.get("critical_count", 0),
                        "high_count": snapshot.get("high_count", 0),
                        "avg_residual": snapshot.get("avg_residual", 0),
                        "avg_inherent": snapshot.get("avg_inherent", 0),
                        "avg_mitigation_strength": snapshot.get("avg_mitigation_strength", 0),
                        "enterprise_health_score": snapshot.get("enterprise_health_score", 0),
                        "treatment_confidence": snapshot.get("treatment_confidence", 0),
                        "top_division": snapshot.get("top_division", "N/A"),
                        "top_division_pct": snapshot.get("top_division_pct", 0),
                        "division_exposure": snapshot.get("division_exposure", {}),
                        "category_exposure": snapshot.get("category_exposure", {}),
                        "pct_within_appetite": snapshot.get("pct_within_appetite", 0),
                        "pct_near_appetite": snapshot.get("pct_near_appetite", 0),
                        "pct_breached": snapshot.get("pct_breached", 0),
                        "emerging_themes": snapshot.get("emerging_themes", []),
                        "board_risks": snapshot.get("board_risks", []),
                        "narrative": narrative,
                    }
                    if st.session_state.tier != "free":
                        excel_data = generate_intelligent_excel_pack(st.session_state.rf_data, narrative)
                        st.download_button("📥 Excel Board Pack", excel_data, file_name=f"RiskForge_{datetime.now().strftime('%Y%m%d')}.xlsx")
                        if st.session_state.tier == "enterprise":
                            pdf_data = generate_pdf_board_pack(narrative, snapshot, st.session_state.org_name, st.session_state.report_title, st.session_state.logo_bytes)
                            st.download_button("📥 PDF Board Pack (Enterprise)", pdf_data, file_name=f"BoardPack_{datetime.now().strftime('%Y%m%d')}.pdf")
                    else:
                        st.info("📌 Upgrade to Professional/Enterprise to download board packs.")
                    st.rerun()

    if st.session_state.rf_data:
        health = st.session_state.rf_data.get("enterprise_health_score", 0)
    else:
        health = 0

    # Determine posture based on global thresholds
    posture = "Unknown"
    posture_color = "#475569"
    posture_bg = "#f1f5f9"
    for score_threshold, label in POSTURE_THRESHOLDS:
        if health >= score_threshold:
            posture = label
            if label == "Strong":
                posture_color, posture_bg = "#10b981", "#d1fae5"
            elif label == "Stable":
                posture_color, posture_bg = "#3b82f6", "#dbeafe"
            elif label == "Elevated":
                posture_color, posture_bg = "#f59e0b", "#fef3c7"
            elif label == "Attention Required":
                posture_color, posture_bg = "#ef4444", "#fee2e2"
            break

    col_logo, col_hero = st.columns([0.5, 5])
    with col_logo:
        if st.session_state.logo_bytes:
            st.image(st.session_state.logo_bytes, width=60)
        else:
            st.markdown("### 🛡️")
    with col_hero:
        st.markdown(f"""
        <div class="exec-hero" style="margin-top: 0;">
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <div style="font-size: 14px; opacity: 0.85; letter-spacing: 0.5px;">
                        {st.session_state.org_name.upper()}
                    </div>
                    <div style="font-size: 36px; font-weight: 700; margin-top: 8px; line-height: 1.1;">
                        {st.session_state.report_title}
                    </div>
                    <div style="margin-top: 12px; font-size: 16px; opacity: 0.9;">
                        Reporting Period: Q{((datetime.now().month-1)//3)+1} {datetime.now().year} • Board Date: {datetime.now().strftime('%B %d, %Y')}
                    </div>
                </div>
                <div style="text-align: right;">
                    <div style="font-size: 14px; opacity: 0.8;">ENTERPRISE HEALTH</div>
                    <div style="font-size: 56px; font-weight: 800; line-height: 1;">{health}</div>
                    <div style="background: {posture_bg}; color: {posture_color}; padding: 6px 16px; border-radius: 40px; font-weight: 700; font-size: 16px; margin-top: 8px; display: inline-block;">
                        {posture} POSTURE
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    render_parser_audit_panel()

    if st.session_state.rf_data:
        data = st.session_state.rf_data
        enterprise_df = data["enterprise_df"]
        raw_df = data["raw_df"]
        clusters_detail_df = data.get("clusters_detail_df", pd.DataFrame())
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "📑 Register", "📈 Intelligence", "🔍 Review Queue", "📤 Export"])

        with tab1:
            st.subheader("Executive Dashboard")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">📋 Enterprise Risks</div>
                    <div class="kpi-value">{data['total_risks']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">⚠️ Critical + High</div>
                    <div class="kpi-value">{data['critical_count'] + data['high_count']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col3:
                health_val = data['enterprise_health_score']
                health_color = "#10b981" if health_val >= 80 else "#3b82f6" if health_val >= 65 else "#f59e0b" if health_val >= 50 else "#ef4444"
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">❤️ Health Score</div>
                    <div class="kpi-value" style="color: {health_color};">{health_val}/100</div>
                </div>
                """, unsafe_allow_html=True)
            with col4:
                conf = data.get('treatment_confidence', 0)
                conf_color = "#10b981" if conf >= 75 else "#f59e0b" if conf >= 50 else "#ef4444"
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">🛠️ Treatment Confidence</div>
                    <div class="kpi-value" style="color: {conf_color};">{conf}%</div>
                </div>
                """, unsafe_allow_html=True)
            with col5:
                st.markdown(f"""
                <div class="kpi-card">
                    <div class="kpi-label">🛡️ Avg Mitigation</div>
                    <div class="kpi-value">{data.get('avg_mitigation_strength', 0)}%</div>
                </div>
                """, unsafe_allow_html=True)

            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                if not enterprise_df.empty:
                    fig_cat = create_category_chart(enterprise_df)
                    st.plotly_chart(fig_cat, use_container_width=True)
            with col_chart2:
                if not enterprise_df.empty:
                    fig_div = create_division_chart(enterprise_df)
                    st.plotly_chart(fig_div, use_container_width=True)
            col_app, col_treat = st.columns(2)
            with col_app:
                if not enterprise_df.empty:
                    fig_app = create_appetite_gauge(enterprise_df, data["threshold"], st.session_state.category_appetite if st.session_state.tier == "enterprise" else None)
                    st.plotly_chart(fig_app, use_container_width=True)
            with col_treat:
                fig_treat = create_treatment_gauge(data.get("treatment_confidence", 0))
                st.plotly_chart(fig_treat, use_container_width=True)
            st.subheader("Risk Appetite Status")
            st.progress(data.get("pct_within_appetite", 0) / 100)
            st.caption(f"Within: {data.get('pct_within_appetite', 0)}% | Near: {data.get('pct_near_appetite', 0)}% | Breached: {data.get('pct_breached', 0)}%")

        with tab2:
            st.subheader("Enterprise Risk Register")

            IMPACT_MAP = {5: "Critical", 4: "Major", 3: "Moderate", 2: "Significant", 1: "Minor"}
            LIKELIHOOD_MAP = {5: "Almost Certain", 4: "Likely", 3: "Moderate", 2: "Unlikely", 1: "Rare"}
            INHERENT_LEVEL_MAP = {
                (20, 25): "High",
                (12, 19): "High",
                (6, 11): "Medium",
                (1, 5): "Low"
            }

            def get_inherent_level(score):
                for (low, high), label in INHERENT_LEVEL_MAP.items():
                    if low <= score <= high:
                        return label
                return "Unknown"

            display_df = enterprise_df.copy()

            display_df["impact_text"] = display_df["impact_pre"].apply(lambda x: IMPACT_MAP.get(int(round(x)), "Unknown") if pd.notna(x) else "Unknown")
            display_df["likelihood_text"] = display_df["likelihood_pre"].apply(lambda x: LIKELIHOOD_MAP.get(int(round(x)), "Unknown") if pd.notna(x) else "Unknown")
            display_df["inherent_text"] = display_df["inherent_score"].apply(get_inherent_level)

            display_cols = [
                "enterprise_risk_id",
                "risk_name",
                "risk_statement",
                "primary_division",
                "primary_category",
                "impact_text",
                "impact_pre",
                "likelihood_text",
                "likelihood_pre",
                "inherent_text",
                "inherent_score",
                "residual_score",
                "primary_owner",
                "primary_strategy",
                "primary_treatment_plan",
                "earliest_due_date",
                "treatment_status",
                "mitigation_strength_pct",
                "cluster_confidence",
            ]
            display_cols = [c for c in display_cols if c in display_df.columns]

            display_df = display_df[display_cols].rename(columns={
                "enterprise_risk_id": "Risk ID",
                "risk_name": "Risk Name",
                "risk_statement": "Risk Statement",
                "primary_division": "Division",
                "primary_category": "Category",
                "impact_text": "IMPACT",
                "impact_pre": "Impact (1-5)",
                "likelihood_text": "LIKELIHOOD",
                "likelihood_pre": "Likelihood (1-5)",
                "inherent_text": "INHERENT RISK",
                "inherent_score": "Inherent Score",
                "residual_score": "Residual Score",
                "primary_owner": "Owner",
                "primary_strategy": "Strategy",
                "primary_treatment_plan": "Treatment Plan",
                "earliest_due_date": "Due Date",
                "treatment_status": "Status",
                "mitigation_strength_pct": "Mitigation %",
                "cluster_confidence": "Confidence",
            })

            threshold = data.get("threshold", st.session_state.board_threshold)
            category_appetite = st.session_state.category_appetite if st.session_state.tier == "enterprise" else {}

            # Build HTML table with improved coloring including dark green for low residual scores
            html_table = '<table class="register-table">'
            html_table += '<thead><tr>'
            for col in display_df.columns:
                html_table += f'<th>{col}</th>'
            html_table += '</thead><tbody>'

            for _, row in display_df.iterrows():
                residual_score = row["Residual Score"]
                # Determine row background based on residual score first (lowest gets dark green)
                if residual_score <= 5:
                    row_bg = "#166534"          # dark green
                elif residual_score <= 8:
                    row_bg = "#15803d"          # medium-dark green
                else:
                    # fallback to appetite-based colors
                    band = appetite_band(residual_score, threshold, row.get("Category", ""), category_appetite)
                    if band in ["breached", "critical breach"]:
                        row_bg = "#fee2e2"       # light red
                    elif band == "near appetite":
                        row_bg = "#fef3c7"       # light yellow
                    elif band == "within appetite":
                        row_bg = "#dcfce7"       # light green
                    else:
                        row_bg = "#ffffff"

                html_table += f'<tr style="background-color: {row_bg};">'
                for col_name, value in row.items():
                    cell_style = ""
                    if col_name == "Confidence":
                        conf = float(value) if pd.notna(value) else 0
                        if conf >= 75:
                            cell_style = "background-color: #dcfce7; color: #166534; font-weight: 600;"
                        elif conf >= 50:
                            cell_style = "background-color: #fef9c3; color: #854d0e; font-weight: 600;"
                        else:
                            cell_style = "background-color: #fee2e2; color: #991b1b; font-weight: 600;"
                    elif col_name == "Status":
                        status_str = str(value).lower()
                        if "overdue" in status_str or "delayed" in status_str:
                            cell_style = "background-color: #fee2e2; color: #991b1b; font-weight: 600;"
                        elif "completed" in status_str or "closed" in status_str:
                            cell_style = "background-color: #dcfce7; color: #166534; font-weight: 600;"
                        elif "on track" in status_str or "active" in status_str:
                            cell_style = "background-color: #fef9c3; color: #854d0e; font-weight: 600;"
                    elif col_name in ["Inherent Score", "Residual Score"]:
                        try:
                            score = float(value)
                            if score <= 5:
                                cell_style = "background-color: #166534; color: white; font-weight: 700;"
                            elif score <= 8:
                                cell_style = "background-color: #15803d; color: white; font-weight: 700;"
                            elif score >= 20:
                                cell_style = "background-color: #dc2626; color: white; font-weight: 700;"
                            elif score >= 12:
                                cell_style = "background-color: #f59e0b; color: white; font-weight: 700;"
                            elif score >= 6:
                                cell_style = "background-color: #fef3c7; color: #92400e; font-weight: 600;"
                        except:
                            pass
                    elif col_name == "Mitigation %":
                        try:
                            mit = float(value)
                            if mit >= 50:
                                cell_style = "background-color: #dcfce7; color: #166534; font-weight: 600;"
                            elif mit >= 25:
                                cell_style = "background-color: #fef9c3; color: #854d0e; font-weight: 600;"
                            else:
                                cell_style = "background-color: #fee2e2; color: #991b1b; font-weight: 600;"
                        except:
                            pass
                    elif col_name == "IMPACT" and str(value) == "Critical":
                        cell_style = "background-color: #fee2e2; font-weight: 700;"
                    elif col_name == "LIKELIHOOD" and str(value) == "Almost Certain":
                        cell_style = "background-color: #fee2e2; font-weight: 700;"
                    elif col_name == "INHERENT RISK" and str(value) == "High":
                        cell_style = "background-color: #ffedd5; font-weight: 700;"

                    display_val = str(value) if pd.notna(value) else ""
                    html_table += f'<td style="{cell_style}">{display_val}</td>'
                html_table += '</tr>'
            html_table += '</tbody></table>'
            st.markdown(html_table, unsafe_allow_html=True)

            with st.expander("📋 Raw Accepted Risks (Source Data)"):
                st.dataframe(raw_df, use_container_width=True)

        with tab3:
            st.markdown(data.get("narrative", "No narrative available"))
            if data.get("board_risks"):
                st.markdown("### Top 5 Board-Attention Risks")
                for risk in data["board_risks"]:
                    st.markdown(f"- **{risk['risk_name']}** ({risk['division']}) – Residual: {risk['residual_score']}/25, Owner: {risk['owner']}, Mitigation: {risk.get('mitigation_strength_pct', 0)}%")

        with tab4:
            st.subheader("🔍 Review Queue – Low Confidence & Rejected Items")
            if not clusters_detail_df.empty:
                low_conf = clusters_detail_df[clusters_detail_df["cluster_confidence"] < 60]
                if not low_conf.empty:
                    st.warning(f"**{len(low_conf)} clusters with confidence < 60%** – Review recommended")
                    st.dataframe(low_conf, use_container_width=True)
                else:
                    st.success("All clusters have acceptable confidence (≥60%).")
            else:
                st.info("No cluster detail available.")

            st.subheader("Rejected Rows")
            if not raw_df.empty:
                low_accept = raw_df[raw_df["acceptance_score"] < 6]
                if not low_accept.empty:
                    st.warning(f"**{len(low_accept)} rows with low acceptance scores**")
                    st.dataframe(low_accept[["risk_name", "risk_statement", "acceptance_score", "acceptance_reason"]], use_container_width=True)
                else:
                    st.success("All accepted rows have good evidence scores.")

        with tab5:
            st.subheader("Export Options")
            if st.session_state.tier != "free":
                st.success("✅ Full export available above.")
            else:
                st.info("📌 Upgrade to download board packs.")

if __name__ == "__main__":
    main()