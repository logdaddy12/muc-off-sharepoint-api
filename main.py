from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Enable CORS (adjust allowed_origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment variables
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SITE_ID = os.getenv("SITE_ID")       # Optional if using dynamic site lookup
DRIVE_ID = os.getenv("DRIVE_ID")     # Optional if you haven't found it yet

# Auth URL and Graph base URL
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Get Microsoft Graph access token
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

# 📍 GET /sharepoint/siteid?search=muc-off
@app.get("/sharepoint/siteid")
async def get_site_id(search: str = "IT"):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{GRAPH_BASE}/sites?search={search}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200:
            print("Site ID Error:", response.text)
            raise HTTPException(status_code=500, detail="Failed to fetch site ID")
        return response.json()

# 📍 GET /sharepoint/files
@app.get("/sharepoint/files")
async def get_sharepoint_files():
    token = await get_token()

    if not SITE_ID or not DRIVE_ID:
        raise HTTPException(status_code=400, detail="SITE_ID or DRIVE_ID not set in .env")

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
