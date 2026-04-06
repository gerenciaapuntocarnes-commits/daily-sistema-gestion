from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import init_db
from routers import router
from crm_router import router as crm_router

app = FastAPI(title="Daily Sistema Gestión API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")
app.include_router(crm_router, prefix="/api")

app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/")
def frontend():
    return FileResponse("daily.html")

@app.on_event("startup")
def startup():
    init_db()
