# api/main.py
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from loguru import logger
from routers import equipment, alerts, kpi

load_dotenv()

app = FastAPI(
    title="Mining Analytics API",
    description="Real-time analytics API for Australian mining equipment",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],    # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(equipment.router)
app.include_router(alerts.router)
app.include_router(kpi.router)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "mining-analytics-api"}

@app.get("/")
async def root():
    return {
        "service":     "Mining Equipment Analytics API",
        "version":     "1.0.0",
        "docs":        "/docs",
        "sites":       ["PILBARA-01", "HUNTER-VALLEY-01", "BOWEN-BASIN-01"],
    }

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(status_code=500, content={"error": str(exc)})
