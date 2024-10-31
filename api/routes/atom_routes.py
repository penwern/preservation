import logging
import requests
from fastapi import APIRouter, HTTPException, Request
from db.models.atom_model import AtomConfigModel
from db.schemas.atom_schema import AtomConfigSchema

logger = logging.getLogger("preservation_api")

router = APIRouter()

@router.get("/", response_model=AtomConfigSchema)
async def get_atom_config():
    logger.info("Getting AtoM config from database")
    try:
        config = AtomConfigModel.get_config_from_db()
        logger.info(config)
        if not config:
            raise HTTPException(status_code=404, detail="Atom config not found")
        config.pop('id')
        return config
    except Exception as e:
        logger.error(f"Exception: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@router.post("/", status_code=201)
async def set_atom_config(config: AtomConfigSchema):
    try:
        data = config.dict()
        logger.info(f"Updating AtoM config in database with data: {data}")
        if AtomConfigModel.get_config_from_db():
            AtomConfigModel.update_config_in_db(data)
            logger.debug("Updated current AtoM config")
            return {"message": "Atom config updated successfully"}
        else:
            AtomConfigModel.add_new_config_to_db(data)
            logger.debug("Added new AtoM config")
            return {"message": "Atom config added successfully"}
    except Exception as e:
        logger.error(f"Exception: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@router.get("/search")
async def search_atom(request: Request):
    try:
        query_string = request.url.query
        logger.info(f"Searching AtoM with parameters: {query_string}")
        atom_config = AtomConfigModel.get_config_from_db()
        
        if not atom_config:
            raise HTTPException(status_code=404, detail="No AtoM config found")
        
        logger.debug(f"AtoM config: {atom_config}")
        
        config = AtomConfigSchema(**atom_config)
        
        atom_api_url = f"{config.atom_url}/api/informationobjects?{query_string}"
        headers = {'REST-API-Key': config.atom_api_key}

        response = requests.get(atom_api_url, headers=headers)

        response.raise_for_status()

        return response.json()

    except Exception as e:
        logger.error(f"Exception: {e}")
        raise
