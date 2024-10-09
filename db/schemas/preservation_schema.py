from pydantic import BaseModel, field_validator
from typing import Literal

class PreservationConfigSchema(BaseModel):
    id: int = None
    name: str
    process_type: Literal['standard', 'eark'] = 'standard'
    compress_aip: Literal[0, 1] = 0
    gen_transfer_struct_report: Literal[0, 1] = 0
    document_empty_directories: Literal[0, 1] = 0
    extract_packages: Literal[0, 1] = 0
    delete_packages_after_extraction: Literal[0, 1] = 0
    normalize: Literal[0, 1] = 0
    compression_level: int = 1
    compression_algorithm: Literal['tar', 'tar_bzip2', 'tar_gzip', 's7_copy', 's7_bzip2', 's7_lzma'] = 's7_bzip2'
    image_normalization_tiff: Literal[0, 1] = 0
    description: str = ''
    user: str
    dip_enabled: Literal[0, 1] = 0

    @field_validator('compression_level')
    def check_compression_level(cls, value):
        if not (1 <= value <= 9):
            raise ValueError('compression_level must be between 1 and 9')
        return value
