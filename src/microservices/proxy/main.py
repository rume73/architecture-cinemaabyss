import os
import random
import logging
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CinemaAbyss Proxy Service")

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

client = httpx.AsyncClient(timeout=60.0)

def should_route_to_movies_service() -> bool:
    if not GRADUAL_MIGRATION:
        return False
    if MOVIES_MIGRATION_PERCENT <= 0:
        return False
    if MOVIES_MIGRATION_PERCENT >= 100:
        return True
    return random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT

def is_movies_endpoint(path: str) -> bool:
    movies_patterns = ["/api/movies", "/api/genres", "/api/ratings", "/api/favorites"]
    for pattern in movies_patterns:
        if path.startswith(pattern):
            return True
    return False

def is_events_endpoint(path: str) -> bool:
    return path.startswith("/api/events")

def get_target_url(path: str) -> tuple:
    if is_events_endpoint(path):
        return EVENTS_SERVICE_URL, "events-service"
    if is_movies_endpoint(path):
        if should_route_to_movies_service():
            return MOVIES_SERVICE_URL, "movies-service"
        else:
            return MONOLITH_URL, "monolith"
    return MONOLITH_URL, "monolith"

@app.get("/")
async def root():
    return {
        "service": "CinemaAbyss Proxy",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "routing": "/_admin/routing",
            "proxy": "/*"
        }
    }

@app.get("/health")
async def health():
    return {"status": "ok", "service": "proxy"}

@app.get("/_admin/routing")
async def routing_info():
    return JSONResponse({
        "gradual_migration": GRADUAL_MIGRATION,
        "migration_percent": MOVIES_MIGRATION_PERCENT,
        "monolith_url": MONOLITH_URL,
        "movies_service_url": MOVIES_SERVICE_URL,
        "events_service_url": EVENTS_SERVICE_URL
    })

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy(request: Request, path: str):
    full_path = f"/{path}" if path else "/"
    target_url, service_name = get_target_url(full_path)
    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)
    target = f"{target_url}{full_path}"

    if service_name == "movies-service":
        logger.info(f"Routing to {service_name}: {request.method} {full_path} (migration: {MOVIES_MIGRATION_PERCENT}%)")
    else:
        logger.info(f"Routing to {service_name}: {request.method} {full_path}")

    try:
        response = await client.request(
            method=request.method,
            url=target,
            headers=headers,
            content=body,
            follow_redirects=True
        )
        logger.info(f"Response from {service_name}: {response.status_code}")
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )
    except httpx.ConnectError:
        logger.error(f"Connection error to {service_name}")
        return JSONResponse(
            status_code=503,
            content={"error": f"Service {service_name} is unavailable", "detail": "Connection refused"}
        )
    except httpx.TimeoutException:
        logger.error(f"Timeout error from {service_name}")
        return JSONResponse(
            status_code=504,
            content={"error": f"Service {service_name} timeout", "detail": "Request took too long"}
        )
    except Exception as e:
        logger.error(f"Error forwarding request to {service_name}: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal proxy error", "detail": str(e)}
        )

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()
