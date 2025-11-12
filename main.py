from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from dotenv import load_dotenv
import pandas as pd

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
        response = await client.post(AUTH_URL, data=data)
        if response.status_code != 200:
            print("Auth Error:", response.text)
            raise HTTPException(status_code=500, detail="Authentication failed")
        return response.json()["access_token"]

@app.get("/sharepoint/siteid")
async def get_site_id(search: str = "muc-off"):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites?search={search}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("Site ID Error:", response.text)
            raise HTTPException(status_code=500, detail="Failed to fetch site ID")
        return response.json()

@app.get("/sharepoint/files")
async def get_sharepoint_files():
    token = await get_token()

    if not SITE_ID or not DRIVE_ID:
        raise HTTPException(status_code=400, detail="SITE_ID or DRIVE_ID not set")

    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{SITE_ID}/drives/{DRIVE_ID}/root/children"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("Drive Error:", response.text)
            raise HTTPException(status_code=500, detail="Failed to fetch files")
        return response.json()["value"]

@app.get("/sharepoint/drives")
async def get_drives():
    token = await get_token()
    if not SITE_ID:
        raise HTTPException(status_code=400, detail="SITE_ID is not set")
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{SITE_ID}/drives"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("Drives Error:", response.text)
            raise HTTPException(status_code=500, detail="Failed to fetch drives")
        return response.json()

@app.get("/sharepoint/folder/{folder_id}/files")
async def get_folder_files(folder_id: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{folder_id}/children"

    files = []
    while url:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            if response.status_code != 200:
                print("Pagination Error:", response.text)
                raise HTTPException(status_code=500, detail="Failed to fetch folder contents")
            data = response.json()
            files.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
    return files

@app.get("/sharepoint/search")
async def search_files(query: str = "", filetype: str = ""):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{SITE_ID}/drives/{DRIVE_ID}/root/search(q='{query}')"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("Search Error:", response.text)
            raise HTTPException(status_code=500, detail="Search failed")
        results = response.json()["value"]
        if filetype:
            results = [f for f in results if f["name"].lower().endswith(filetype.lower())]
        return results

# 📥 New: Download and parse Excel file content
@app.get("/sharepoint/file/{item_id}/excel")
async def read_excel_file(item_id: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{item_id}/content"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("Download Error:", response.text)
            raise HTTPException(status_code=500, detail="Failed to download Excel file")

        # Save temporary Excel file
        with open("temp.xlsx", "wb") as f:
            f.write(response.content)

    try:
        df = pd.read_excel("temp.xlsx")
        suppliers = df.iloc[:, 0].dropna().unique().tolist()
        return {
            "sample_suppliers": suppliers[:10],
            "total_suppliers": len(suppliers)
        }
    except Exception as e:
        print("Excel parsing error:", str(e))
        raise HTTPException(status_code=500, detail="Failed to parse Excel file")
