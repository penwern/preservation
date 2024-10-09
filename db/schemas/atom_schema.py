from pydantic import BaseModel, validator

class AtomConfigSchema(BaseModel):
    atom_url: str
    atom_api_key: str
    atom_username: str
    atom_password: str
    
    @validator('atom_url')
    def check_site_url(cls, value):
        if not value.startswith('https://'):
            raise ValueError("site_url must start with 'https://'.")
        return value

    @validator('atom_api_key')
    def check_api_key(cls, value):
        if not value.isalnum():
            raise ValueError("api_key must be alphanumeric.")
        return value
