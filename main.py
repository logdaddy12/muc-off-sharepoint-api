from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment vars
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_ID = os.getenv("SITE_ID")
DRIVE_ID = os.getenv("DRIVE_ID")

AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Get Graph API token
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

# 📂 List root files/folders
@app.get("/sharepoint/files")
async def get_root_files():
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{SITE_ID}/drives/{DRIVE_ID}/root/children"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("File list error:", response.text)
            raise HTTPException(status_code=500, detail="Failed to get files")
        return response.json()["value"]

# 📂 List contents of a folder by ID (with pagination)
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
                print("Folder read error:", response.text)
                raise HTTPException(status_code=500, detail="Failed to get folder contents")
            data = response.json()
            files.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
    return files

# 🔍 Search files by name and/or file type
@app.get("/sharepoint/search")
async def search_files(query: str = "", filetype: str = ""):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites/{SITE_ID}/drives/{DRIVE_ID}/root/search(q='{query}')"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("Search error:", response.text)
            raise HTTPException(status_code=500, detail="Search failed")
        results = response.json()["value"]
        if filetype:
            results = [f for f in results if f["name"].lower().endswith(filetype.lower())]
        return results

# 📊 Read and return supplier data from Excel inside a folder
@app.get("/sharepoint/folder/{folder_id}/file/{file_name}/excel")
async def read_excel_from_folder(folder_id: str, file_name: str):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    # Get all items in folder
    url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{folder_id}/children"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to read folder")
        items = response.json().get("value", [])

    # Match file
    match = next((item for item in items if item["name"].lower() == file_name.lower()), None)
    if not match:
        raise HTTPException(status_code=404, detail=f"File '{file_name}' not found in folder")

    file_id = match["id"]
    download_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{file_id}/content"

    # Download file
    async with httpx.AsyncClient() as client:
        file_response = await client.get(download_url, headers=headers)
        if file_response.status_code != 200:
            raise HTTPException(status_code=500, detail="Failed to download file")
        with open("temp.xlsx", "wb") as f:
            f.write(file_response.content)

    # Parse Excel
    try:
        df = pd.read_excel("temp.xlsx")
        suppliers = df.iloc[:, 0].dropna().unique().tolist()
        return {
            "sample_suppliers": suppliers[:10],
            "total_suppliers": len(suppliers),
            "source_file": file_name
        }
    except Exception as e:
        print("Excel parse error:", str(e))
        raise HTTPException(status_code=500, detail="Excel parsing failed")
