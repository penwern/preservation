from fastapi import APIRouter, HTTPException
from db.models.preservation_model import PreservationConfigModel
from db.schemas.preservation_schema import PreservationConfigSchema
import logging

logger = logging.getLogger("preservation_api")

router = APIRouter()

@router.get("/", response_model=list[PreservationConfigSchema])
async def get_all_preservation_configs():
    logger.info("Getting all preservation configs from database")
    try:
        configs = PreservationConfigModel.get_all_configs_from_db()
        logger.debug(f"Preservation configs: {configs}")
        return configs
    except Exception as e:
        logger.error(f"Preservation: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@router.post("/", status_code=201)
async def add_preservation_config_to_db(config: PreservationConfigSchema):
    logger.info("Adding / updating new preservation config to database")
    try:
        data = config.dict()
        logger.debug(f"Received preservation data: {data}")

        if 'id' in data and data['id']:
            logger.info(f"Updating preservation config with ID: {data['id']}")
            logger.debug(f"Received data: {data}")
            if PreservationConfigModel.get_config_from_db(data['id']):
                PreservationConfigModel.update_config_in_db(data)
                logger.info("Preservation config updated successfully")
                return {"message": "Preservation config updated successfully"}
            else:
                logger.info("Preservation config not found")
                raise HTTPException(status_code=404, detail="Preservation config ID not found")
        else:
            logger.info("Adding new preservation config")
            logger.debug(f"Received data: {data}")
            PreservationConfigModel.add_new_config_to_db(data)
            logger.info("Preservation config added successfully")
            return {"message": "Preservation config added successfully"}
    except Exception as e:
        logger.error(f"Preservation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/{id}", status_code=200)
async def update_preservation_config(id: int, config: PreservationConfigSchema):
    if id == 1:
        logger.error(f"Can't update default configuration. ID: {id}")
        raise HTTPException(status_code=404, detail="Can't update default config")
    logger.info(f"Updating preservation config with ID: {id}")
    try:
        data = config.dict()
        logger.info(f"Received data: {data}")

        if PreservationConfigModel.get_config_from_db(id):
            PreservationConfigModel.update_config_in_db(data, id)
            return {"message": "Preservation config updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="Preservation config ID not found")
    except Exception as e:
        logger.error(f"Preservation: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@router.delete("/{id}", status_code=200)
async def delete_preservation_config(id: int):
    if id == 1:
        logger.error(f"Can't delete default configuration. ID: {id}")
        raise HTTPException(status_code=403, detail="Can't delete default config")
    logger.info(f"Deleting preservation config with ID: {id}")
    try:
        if PreservationConfigModel.get_config_from_db(id):
            PreservationConfigModel.delete_config_from_db(id)
            return {"message": "Preservation config deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Preservation config ID not found")
    except Exception as e:
        logger.error(f"Preservation: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
