import logging
from fastapi import FastAPI, Request

from config import CURATE_URL, LOG_DIRECTORY
from db.models.atom_model import init_db as init_atom_db
from db.models.preservation_model import init_db as init_preservation_db
from api.routes.preservation_routes import router as preservation_router
from api.routes.atom_routes import router as atom_router

logger = logging.getLogger("preservation_api")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(f'{LOG_DIRECTORY}/preservation_api.log')
file_handler.setFormatter(logging.Formatter("%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(file_handler)

description = """
## Preservation Configs

* **Get all configs** (`GET /preservation`).
* **Add new configs** (`POST /preservation`).
* **Update configs** (`POST /preservation/{id}`).
* **Delete configs** (`DELETE /preservation/{id}`).

## AtoM Configs

* **Get config** (`GET /atom`).
* **Add / Update config** (`POST /atom`).

## Authentication
You will need to authenticate with a valid token. The token is passed in the `Authorization` header as `Bearer <token>`.
"""

app = FastAPI(
    title="Penwern Curate Preservation API",
    description=description,
    summary="Penwern Curate Preservation API is a RESTful API for managing preservation configurations.",
    version="0.1.0",
    contact={
        "name": "Curate",
        "url": CURATE_URL,
        # "email": "support@penwern.co.uk",
    }
)

@app.on_event("startup")
async def startup_event():
    init_atom_db()
    init_preservation_db()
    logger.info("App started")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info("Request made")
    response = await call_next(request)
    return response

@app.get("/", tags=["Default"])
async def root():
    return {"message": "Hello from the API!"}

try:
    app.include_router(preservation_router, prefix="/preservation", tags=["Preservation"])
    app.include_router(atom_router, prefix="/atom", tags=["AtoM"])
except Exception as e:
    logger.error(e)
    raise
