import subprocess
import urllib.parse
import requests
from pathlib import Path
from urllib.parse import urlparse

class AtoMManager():
    def __init__(self, atom_config: dict):
        self.atom_url = atom_config['url']
        self.atom_api = atom_config['api_key']
        self.atom_username = atom_config['username']
        self.atom_password = atom_config['password']
    
    def upload_dip(self, dip_path: Path, slug: str):
        # Get hostname
        hostname = urlparse(self.atom_url).netloc

        # RSync to AtoM
        rsync_command = ["rsync", "-avz", str(dip_path), f"archivematica@{hostname}:/home/archivematica/atom_sword_deposit/"]
        print(rsync_command)
        
        # Execute RSync
        subprocess.run(rsync_command, capture_output=True, text=True, check=True)
        
        self._deposit_dip(Path(dip_path), slug)

    def _deposit_dip(self, dip_path: Path, slug: str):
        deposit_url = f"{self.atom_url}/sword/deposit/{slug}"
        
        encoded_string = f"file:///{urllib.parse.quote_plus(dip_path.name)}"
        
        headers = {
            'Content-Location': encoded_string,
            'X-Packaging': 'http://purl.org/net/sword-types/METSArchivematicaDIP',
            'X-No-Op': 'false',
            'User-Agent': 'curate',
            'Content-Type': 'application/zip'
        }
        auth = requests.auth.HTTPBasicAuth(self.atom_username, self.atom_password)
        response = requests.request("POST", deposit_url, headers=headers, auth=auth, allow_redirects=False)
        
        response.raise_for_status()
