from __future__ import annotations

import os
import re
import base64
import tempfile
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from dotenv import load_dotenv
from docx import Document
import fitz  # PyMuPDF

# =========================
# Boot & Config
# =========================

load_dotenv()

APP_NAME = os.getenv("APP_NAME", "Muc-Off SharePoint API")
TENANT_ID = os.getenv("TENANT_ID", "")
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
DEFAULT_SITE_ID = os.getenv("SITE_ID", "") or None
DEFAULT_DRIVE_ID = os.getenv("DRIVE_ID", "") or None

# CORS: comma-separated origins; default to none (deny all)
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]

# Optional: lock to specific site IDs (comma-separated). Leave empty to allow all sites.
ALLOWED_SITE_IDS = {s.strip() for s in os.getenv("ALLOWED_SITE_IDS", "").split(",") if s.strip()}

# HTTP settings
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))  # seconds
HTTP_MAX_REDIRECTS = int(os.getenv("HTTP_MAX_REDIRECTS", "5"))

AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(APP_NAME)

# FastAPI app
app = FastAPI(title=APP_NAME)

# Security-first CORS: default deny unless ALLOWED_ORIGINS provided
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS else [],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


# =========================
# Token Cache
# =========================

class TokenCache:
    """Simple in-memory token cache for client-credentials flow."""
    _token: Optional[str] = None
    _expires_at: Optional[datetime] = None

    @classmethod
    async def get_token(cls) -> str:
        if cls._token and cls._expires_at and datetime.utcnow() < cls._expires_at:
            return cls._token

        if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
            log.error("Missing OAuth environment variables")
            raise HTTPException(status_code=500, detail="Server authentication not configured")

        data = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.post(AUTH_URL, data=data)
            if resp.status_code != 200:
                log.error("Auth failed: %s", resp.text[:400])
                raise HTTPException(status_code=502, detail="Upstream auth error")

            payload = resp.json()
            cls._token = payload.get("access_token")
            # Buffer expiry by 60 seconds
            expires_in = int(payload.get("expires_in", 3600))
            cls._expires_at = datetime.utcnow() + timedelta(seconds=max(0, expires_in - 60))

            if not cls._token:
                log.error("Auth response missing access_token")
                raise HTTPException(status_code=502, detail="Upstream auth error")

            return cls._token


# =========================
# HTTP / Graph Helpers
# =========================

def _sanitize_graph_error(text: str) -> str:
    # Return a generic message without leaking internals
    return "Microsoft Graph request failed"

async def graph_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    token = await TokenCache.get_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = path if path.startswith("http") else f"{GRAPH_BASE}{path}"
    # follow_redirects enables /content resolution
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True, limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)) as client:
        resp = await client.get(url, headers=headers, params=params)
        if resp.status_code >= 400:
            log.warning("Graph GET %s -> %s", url, resp.status_code)
            # Prefer not to echo full error to clients
            raise HTTPException(status_code=502, detail=_sanitize_graph_error(resp.text))
        # If JSON expected but content-type isn't JSON, caller should handle bytes
        ctype = resp.headers.get("content-type", "")
        if "application/json" in ctype or "text/json" in ctype or resp.text.startswith("{"):
            try:
                return resp.json()
            except Exception:
                # Some list endpoints always return JSON; if parse fails, treat as 502
                raise HTTPException(status_code=502, detail="Invalid JSON from Microsoft Graph")
        else:
            # Return raw in a wrapper for content endpoints
            return {"_raw_bytes": resp.content, "_headers": dict(resp.headers)}

async def graph_get_bytes(path: str) -> bytes:
    token = await TokenCache.get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = path if path.startswith("http") else f"{GRAPH_BASE}{path}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code >= 400:
            log.warning("Graph GET (bytes) %s -> %s", url, resp.status_code)
            raise HTTPException(status_code=502, detail=_sanitize_graph_error(resp.text))
        return resp.content


# =========================
# Models & Validators
# =========================

GUID_RE = re.compile(r"^[A-Za-z0-9\-_!.:]+$")  # lenient: Graph IDs are not strict GUIDs

def _validate_id(value: Optional[str], name: str) -> Optional[str]:
    if value is None:
        return None
    if not GUID_RE.match(value):
        raise HTTPException(status_code=400, detail=f"Invalid {name}")
    return value

class ExcelQuery(BaseModel):
    cardcode: Optional[str] = None
    min_total: Optional[float] = Query(default=None, ge=0)
    max_total: Optional[float] = Query(default=None, ge=0)
    start_date: Optional[str] = None  # YYYY-MM-DD
    end_date: Optional[str] = None    # YYYY-MM-DD

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Dates must be in YYYY-MM-DD format")

    @field_validator("max_total")
    @classmethod
    def check_ranges(cls, v, info):
        min_total = info.data.get("min_total")
        if v is not None and min_total is not None and v < min_total:
            raise ValueError("max_total must be >= min_total")
        return v


# =========================
# Guards
# =========================

def enforce_site_allowed(site_id: Optional[str]) -> None:
    if not site_id or not ALLOWED_SITE_IDS:
        # If no site restrictions configured, allow all
        return
    if site_id not in ALLOWED_SITE_IDS:
        raise HTTPException(status_code=403, detail="Site access not permitted")

def ensure_defaults(site_id: Optional[str], drive_id: Optional[str]) -> (Optional[str], Optional[str]):
    # Use defaults if provided in env (kept for backward compatibility)
    return site_id or DEFAULT_SITE_ID, drive_id or DEFAULT_DRIVE_ID


# =========================
# Endpoints: Sites & Drives
# =========================

@app.get("/sharepoint/sites")
async def list_sites():
    """List all accessible SharePoint sites."""
    data = await graph_get("/sites?search=*")
    return data.get("value", [])

@app.get("/sharepoint/site/{site_id}/drives")
async def list_drives(site_id: str):
    site_id = _validate_id(site_id, "site_id")
    enforce_site_allowed(site_id)
    data = await graph_get(f"/sites/{site_id}/drives")
    return data.get("value", [])


# =========================
# Endpoints: Files & Search (with optional defaults)
# =========================

@app.get("/sharepoint/files")
async def list_files(site_id: Optional[str] = None, drive_id: Optional[str] = None):
    site_id, drive_id = ensure_defaults(_validate_id(site_id, "site_id"), _validate_id(drive_id, "drive_id"))
    if not (site_id and drive_id):
        raise HTTPException(status_code=400, detail="site_id and drive_id are required (no defaults configured)")
    enforce_site_allowed(site_id)
    data = await graph_get(f"/sites/{site_id}/drives/{drive_id}/root/children")
    return data.get("value", [])

@app.get("/sharepoint/search")
async def search_files(query: str = Query(..., min_length=1), site_id: Optional[str] = None, drive_id: Optional[str] = None):
    site_id, drive_id = ensure_defaults(_validate_id(site_id, "site_id"), _validate_id(drive_id, "drive_id"))
    if not (site_id and drive_id):
        raise HTTPException(status_code=400, detail="site_id and drive_id are required (no defaults configured)")
    enforce_site_allowed(site_id)
    # Graph search within a drive
    path = f"/sites/{site_id}/drives/{drive_id}/root/search(q='{query}')"
    data = await graph_get(path)
    return data.get("value", [])


# =========================
# Endpoints: Raw Content & Text Extraction
# =========================

@app.get("/sharepoint/site/{site_id}/drive/{drive_id}/file/{item_id}/content")
async def get_file_content(site_id: str, drive_id: str, item_id: str):
    site_id = _validate_id(site_id, "site_id")
    drive_id = _validate_id(drive_id, "drive_id")
    item_id = _validate_id(item_id, "item_id")
    enforce_site_allowed(site_id)

    content = await graph_get_bytes(f"/sites/{site_id}/drives/{drive_id}/items/{item_id}/content")
    b64 = base64.b64encode(content).decode("utf-8")
    return {
        "file_id": item_id,
        "file_type": "binary",
        "base64_content": b64,
        "size_bytes": len(content),
    }

@app.get("/sharepoint/site/{site_id}/drive/{drive_id}/file/{item_id}/text")
async def extract_text(site_id: str, drive_id: str, item_id: str, filetype: str = Query("pdf", pattern="^(pdf|docx)$")):
    site_id = _validate_id(site_id, "site_id")
    drive_id = _validate_id(drive_id, "drive_id")
    item_id = _validate_id(item_id, "item_id")
    enforce_site_allowed(site_id)

    content = await graph_get_bytes(f"/sites/{site_id}/drives/{drive_id}/items/{item_id}/content")

    suffix = f".{filetype}"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        if filetype == "docx":
            text = "\n".join(p.text for p in Document(tmp_path).paragraphs)
        else:
            with fitz.open(tmp_path) as pdf:
                text = "\n".join(page.get_text() for page in pdf)
    except Exception as e:
        log.warning("Text extraction failed: %s", str(e))
        raise HTTPException(status_code=500, detail="Failed to parse document")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    text = text or ""
    return {"content": text[:3000], "length": len(text), "source_filetype": filetype}


# =========================
# Endpoint: Excel (SAP-aware, filters, grouping)
# =========================

def _detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Detects SAP-style columns dynamically (case-insensitive, substring and synonym matches).
    Works for both supplier (AP) and customer (AR) exports.
    """
    cols = [str(c).strip().lower() for c in df.columns]
    mapping: Dict[str, str] = {}

    def find(*keys: str) -> Optional[str]:
        """Return first matching column for any key fragment."""
        for k in keys:
            for c in cols:
                # Match exact or substring (case-insensitive)
                if k in c:
                    return c
        return None

    # === Business Partner Fields (AP + AR) ===
    mapping["cardcode"] = find(
        "cardcode", "vendor", "bpcode", "supplierid", "customer", "clientcode", "debitor", "partnercode"
    )
    mapping["cardname"] = find(
        "cardname", "vendorname", "bpname", "suppliername", "customername", "clientname", "debitorname", "partnername"
    )

    # === Document Identifiers ===
    mapping["docnum"] = find(
        "docnum", "docentry", "invoice", "invno", "invnum", "documentnumber", "po_no",
        "po number", "doc no", "order", "salesorder", "purchaseorder"
    )

    # === Dates ===
    mapping["date"] = find(
        "docdate", "taxdate", "postingdate", "posting", "duedate", "createdate", "date", "trandate"
    )

    # === Totals and Amounts (prefer detailed fields) ===
    mapping["total"] = find(
        "linetotal", "doctotal", "totalamount", "amount", "grandtotal", "total", "netvalue",
        "grossamount", "debit", "credit", "balance", "priceaftervat", "netamt"
    )

    # === Quantities ===
    mapping["qty"] = find(
        "quantity", "qty", "openqty", "baseqty", "shipqty", "delqty", "invoicedqty", "orderedqty"
    )

    # === Items / Materials ===
    mapping["item"] = find(
        "itemcode", "item", "dscription", "description", "material", "sku", "product", "partnumber", "materialcode"
    )

    # === Financial Details (optional enrichments) ===
    mapping["tax"] = find("tax", "vat", "gst", "taxamt", "taxamount")
    mapping["discount"] = find("disc", "discount", "discperc", "discamt", "discountamount")
    mapping["currency"] = find("currency", "curr", "currcode")
    mapping["warehouse"] = find("whscode", "warehouse", "location")
    mapping["costcenter"] = find("costcenter", "profitcenter", "costctr", "pc", "division")
    mapping["cardtype"] = find("cardtype", "bptype", "businesspartner", "bpgroup")

    # === User-defined (custom) fields ===
    for c in cols:
        if c.startswith("u_") and "userfields" not in mapping:
            mapping["userfields"] = c
            break

    # Filter out None values
    return {k: v for k, v in mapping.items() if v}


def _parse_datesafe(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d")

@app.get("/sharepoint/site/{site_id}/drive/{drive_id}/file/{item_id}/excel")
async def analyze_excel(
    site_id: str,
    drive_id: str,
    item_id: str,
    cardcode: Optional[str] = None,
    min_total: Optional[float] = Query(default=None, ge=0),
    max_total: Optional[float] = Query(default=None, ge=0),
    start_date: Optional[str] = None,  # YYYY-MM-DD
    end_date: Optional[str] = None,    # YYYY-MM-DD
):
    site_id = _validate_id(site_id, "site_id")
    drive_id = _validate_id(drive_id, "drive_id")
    item_id = _validate_id(item_id, "item_id")
    enforce_site_allowed(site_id)

    # Fetch file
    content = await graph_get_bytes(f"/sites/{site_id}/drives/{drive_id}/items/{item_id}/content")

    # Read Excel to temp
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        df = pd.read_excel(tmp_path)
    except Exception as e:
        log.warning("Excel read failed: %s", str(e))
        raise HTTPException(status_code=400, detail="Unsupported or corrupt Excel file")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    if df.empty:
        return {
            "filtered_by": {
                "cardcode": cardcode,
                "min_total": min_total,
                "max_total": max_total,
                "start_date": start_date,
                "end_date": end_date,
            },
            "fields_detected": [],
            "supplier_totals": [],
            "sample_records": [],
            "total_records": 0,
        }

    # Normalize headers
    df.columns = [str(c).strip().lower() for c in df.columns]
    colmap = _detect_columns(df)

    # Normalize numeric totals
    if "total" in colmap:
        df[colmap["total"]] = pd.to_numeric(df[colmap["total"]], errors="coerce")

    # Normalize dates
    if "date" in colmap:
        df[colmap["date"]] = pd.to_datetime(df[colmap["date"]], errors="coerce")

    # Filters
    if cardcode and "cardcode" in colmap:
        df = df[df[colmap["cardcode"]].astype(str).str.contains(cardcode, case=False, na=False)]

    if min_total is not None and "total" in colmap:
        df = df[df[colmap["total"]] >= float(min_total)]
    if max_total is not None and "total" in colmap:
        df = df[df[colmap["total"]] <= float(max_total)]

    sd = _parse_datesafe(start_date)
    ed = _parse_datesafe(end_date)
    if sd and "date" in colmap:
        df = df[df[colmap["date"]] >= sd]
    if ed and "date" in colmap:
        df = df[df[colmap["date"]] <= ed]

    total_records = int(len(df))

    # Supplier totals
    supplier_totals: List[Dict[str, Any]] = []
    if "cardcode" in colmap and "total" in colmap and total_records > 0:
        group_keys = [colmap["cardcode"]]
        if "cardname" in colmap:
            group_keys.append(colmap["cardname"])

        grouped = (
            df.groupby(group_keys, dropna=False)[colmap["total"]]
            .sum()
            .reset_index()
        )

        # Rename to stable keys
        renames = {
            colmap["cardcode"]: "CardCode",
            colmap.get("cardname", colmap["cardcode"]): "CardName",
            colmap["total"]: "TotalAmount",
        }
        supplier_totals = grouped.rename(columns=renames).to_dict(orient="records")

    # Sample preview (limited, to avoid PII leakage)
    preview_cols = [colmap.get(k) for k in ["cardcode", "cardname", "docnum", "total", "date"] if colmap.get(k)]
    sample_records = df[preview_cols].head(10).to_dict(orient="records") if preview_cols else []

    return {
        "filtered_by": {
            "cardcode": cardcode,
            "min_total": min_total,
            "max_total": max_total,
            "start_date": start_date,
            "end_date": end_date,
        },
        "fields_detected": list(colmap.keys()),
        "supplier_totals": supplier_totals,
        "sample_records": sample_records,
        "total_records": total_records,
    }


# =========================
# Health
# =========================

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
