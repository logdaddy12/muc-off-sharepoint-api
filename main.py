from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import tempfile
import base64
from dotenv import load_dotenv
import pandas as pd
from docx import Document
import fitz  # PyMuPDF

load_dotenv()
app = FastAPI()

# Allow requests from anywhere (for GPT + testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment defaults
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DEFAULT_SITE_ID = os.getenv("SITE_ID")
DEFAULT_DRIVE_ID = os.getenv("DRIVE_ID")

AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"


# 🔑 Acquire Graph token
async def get_token():
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(AUTH_URL, data=data)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Auth failed: {resp.text}")
        return resp.json()["access_token"]


# 🌐 List all SharePoint sites
@app.get("/sharepoint/sites")
async def list_sites():
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites?search=*"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=resp.text)
        return resp.json().get("value", [])


# 📂 List drives (document libraries) for a given site
@app.get("/sharepoint/site/{site_id}/drives")
async def list_drives(site_id: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{site_id}/drives"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=resp.text)
        return resp.json().get("value", [])


# 📁 List files (either default site or specified site/drive)
@app.get("/sharepoint/files")
async def list_files(site_id: str = None, drive_id: str = None):
    token = await get_token()
    site_id = site_id or DEFAULT_SITE_ID
    drive_id = drive_id or DEFAULT_DRIVE_ID
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/root/children"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=resp.text)
        return resp.json().get("value", [])


# 🔍 Search files within a site/drive
@app.get("/sharepoint/search")
async def search_files(query: str, site_id: str = None, drive_id: str = None):
    token = await get_token()
    site_id = site_id or DEFAULT_SITE_ID
    drive_id = drive_id or DEFAULT_DRIVE_ID
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/root/search(q='{query}')"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=resp.text)
        return resp.json().get("value", [])


# 🧾 Extract text from DOCX or PDF file
@app.get("/sharepoint/site/{site_id}/drive/{drive_id}/file/{item_id}/text")
async def extract_text(site_id: str, drive_id: str, item_id: str, filetype: str = "pdf"):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, follow_redirects=True)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="File download failed")
        with tempfile.NamedTemporaryFile(suffix=f".{filetype}", delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

    try:
        if filetype == "docx":
            text = "\n".join([p.text for p in Document(tmp_path).paragraphs])
        elif filetype == "pdf":
            with fitz.open(tmp_path) as pdf:
                text = "\n".join([page.get_text() for page in pdf])
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
    finally:
        os.remove(tmp_path)

    return {"content": text[:3000], "length": len(text), "source_filetype": filetype}


# 📊 Read Excel from any site/drive
@app.get("/sharepoint/site/{site_id}/drive/{drive_id}/file/{item_id}/excel")
async def read_excel(site_id: str, drive_id: str, item_id: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    async with httpx.AsyncClient() as client:
        file_resp = await client.get(url, headers=headers, follow_redirects=True)
        if file_resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to download Excel")

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(file_resp.content)
            tmp_path = tmp.name

    try:
        df = pd.read_excel(tmp_path)
        suppliers = df.iloc[:, 0].dropna().unique().tolist()
    finally:
        os.remove(tmp_path)

    return {
        "sample_suppliers": suppliers[:10],
        "total_suppliers": len(suppliers)
    }


# 🧠 Raw binary content (base64)
@app.get("/sharepoint/site/{site_id}/drive/{drive_id}/file/{item_id}/content")
async def get_file_content(site_id: str, drive_id: str, item_id: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers, follow_redirects=True)
        if r.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to get file content")
    b64 = base64.b64encode(r.content).decode("utf-8")
    return {
        "file_id": item_id,
        "file_type": "binary",
        "base64_content": b64,
        "size_bytes": len(r.content)
    }