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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_ID = os.getenv("SITE_ID")
DRIVE_ID = os.getenv("DRIVE_ID")

AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"


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


@app.get("/sharepoint/files")
async def list_root_files():
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{SITE_ID}/drives/{DRIVE_ID}/root/children"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=resp.text)
        return resp.json().get("value", [])


@app.get("/sharepoint/folder/{folder_id}/files")
async def list_folder_files(folder_id: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{folder_id}/children"
    results = []
    async with httpx.AsyncClient() as client:
        while url:
            r = await client.get(url, headers=headers)
            if r.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to get folder contents")
            data = r.json()
            results.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
    return results


@app.get("/sharepoint/search")
async def search_files(query: str = "", filetype: str = ""):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{SITE_ID}/drives/{DRIVE_ID}/root/search(q='{query}')"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers)
        if r.status_code != 200:
            raise HTTPException(status_code=500, detail=r.text)
        results = r.json().get("value", [])
        if filetype:
            results = [f for f in results if f["name"].lower().endswith(filetype.lower())]
        return results


@app.get("/sharepoint/folder/{folder_id}/file/{file_name}/excel")
async def parse_excel(folder_id: str, file_name: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    list_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{folder_id}/children"

    async with httpx.AsyncClient() as client:
        resp = await client.get(list_url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to list folder")
        items = resp.json().get("value", [])
    match = next((i for i in items if i["name"].lower() == file_name.lower()), None)
    if not match:
        raise HTTPException(status_code=404, detail="File not found")
    file_id = match["id"]

    download_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{file_id}/content"
    async with httpx.AsyncClient() as client:
        file_resp = await client.get(download_url, headers=headers, follow_redirects=True)
        if file_resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Download failed")
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
        "total_suppliers": len(suppliers),
        "source_file": file_name
    }


@app.get("/sharepoint/file/{item_id}/text")
async def extract_text(item_id: str, filetype: str = "pdf"):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    file_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{item_id}/content"

    async with httpx.AsyncClient() as client:
        resp = await client.get(file_url, headers=headers, follow_redirects=True)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Download failed")
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


@app.get("/sharepoint/file/{item_id}/content")
async def get_file_content(item_id: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{item_id}/content"
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers, follow_redirects=True)
        if r.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to fetch file content")
    b64 = base64.b64encode(r.content).decode("utf-8")
    return {
        "file_id": item_id,
        "file_type": "binary",
        "base64_content": b64,
        "size_bytes": len(r.content)
    }
