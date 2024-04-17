import json
import logging
import subprocess
import requests
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("preservation")

token_timeout_minutes = 5

class CurateManager():
    def __init__(self, user: str, curate_url: str):
        self._user: str = user
        self._url: str = curate_url
        self._token: str = None
        self._token_timeout: datetime = None
        self._gen_new_token()
        
    @property
    def token(self) -> str:
        if not self._token or self._has_expired():
            self._gen_new_token()
        return self._token
    
    def _has_expired(self) -> bool:
        return self._token_timeout < datetime.now()
        
    def _gen_new_token(self) -> str:
        commands = ['cells', 'admin', 'user', 'token', '-u', self._user, '-e', f'{token_timeout_minutes}m', '--quiet']
        result = subprocess.run(commands, capture_output=True, text=True, check=True)
        if result.returncode != 0:
            raise RuntimeError("Failed to generate token.")
        token = result.stdout.strip()
        if not token:
            raise RuntimeError("Generated token is empty.")
        self._token = token
        self._token_timeout = datetime.now() + timedelta(minutes=token_timeout_minutes)
        self._configure_cells_client()
        return token

    def _configure_cells_client(self):
        """
        Configure Cells Client.
        """
        commands = ['cec', 'configure', 'token', '--url', self._url, '--login', self._user, '--token', self.token]
        output = subprocess.run(commands, capture_output=True, text=True, check=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to configure Cells Client.")

    def update_tag(self, node_id: str, tag: str):
        """
        Uses pydio API to update a3m-progress tag.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        payload = json.dumps({
            "MetaDatas": [
                {
                    "JsonValue": f"\"{tag}\"",
                    "Namespace": "usermeta-a3m-progress",
                    "NodeUuid": node_id
                }
            ],
            "Operation": "PUT"
        })
        endpoint = f'{self._url}/a/user-meta/update'
        response = requests.request("PUT", endpoint, headers=headers, data=payload)
        response.raise_for_status()
        logger.debug(f"Tag: {tag}")
    
    def gather_child_nodes(self, parent_curate_node_path: str) -> list:
        """
        Uses pydio API to gather child nodes of a parent.
        """
        logger.info(f"Gathering children of {parent_curate_node_path}")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        payload = json.dumps({
            "Node": {
                "Path": str(parent_curate_node_path)
            },
            "Recursive": True
        })
        endpoint = f"{self._url}/a/tree/admin/list"
        response = requests.request("POST", endpoint, headers=headers, data=payload)
        response.raise_for_status()
        
        # Only return the children
        # We could keep the parent but dont need to until
        # we input lists of node paths instead of 
        # whole node json
        return response.json().get('Children', [])

    def download_node(self, destination_path: Path, node_path: Path):
        # Download node using Cells Client
        destination_path.mkdir(parents=True, exist_ok=True)
        commands = ['cec', 'scp', f'cells:///{str(node_path)}', str(destination_path)]
        subprocess.run(commands, capture_output=True, text=True, check=True)
        
        # Single file/folder download expected
        downloaded_files = list(destination_path.iterdir())
        if len(downloaded_files) == 1:
            downloaded_file = downloaded_files[0]
            logger.info(f"Downloaded {downloaded_file}")
            return Path(downloaded_file)
        else:
            raise ValueError("Expected a single file or folder to be downloaded.")

    def upload_node(self, file_path: Path, curate_destination: str) -> Path:
        subprocess.run(
            ['cec', 'scp', str(file_path), f'cells://{curate_destination}/'],
            capture_output=True,
            text=True,
            check=True
        )
        return Path(curate_destination) / file_path.name
