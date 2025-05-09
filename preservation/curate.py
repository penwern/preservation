import json
import logging
import subprocess
import requests
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("preservation")

token_timeout_minutes = 5

class CurateManager:
    def __init__(self, user: str, curate_url: str):
        self._user: str = user
        self._url: str = curate_url
        self._token: str = None
        self._token_timeout: datetime = datetime.min
        self._admin_token: str = None
        self._admin_token_timeout: datetime = datetime.min
        self._configure_cells_client()

    def token(self, user) -> str:
        if user == 'admin':
            if self._has_expired(self._admin_token_timeout):
                self._admin_token = self._gen_new_token('admin')
                self._admin_token_timeout = datetime.now() + timedelta(minutes=token_timeout_minutes)
            return self._admin_token
        else:
            if self._has_expired(self._token_timeout):
                self._token = self._gen_new_token(user)
                self._token_timeout = datetime.now() + timedelta(minutes=token_timeout_minutes)
            return self._token

    def _has_expired(self, timeout) -> bool:
        return datetime.now() >= timeout

    def _gen_new_token(self, user):
        commands = ['cells', 'admin', 'user', 'token', '-u', user, '-e', f'{token_timeout_minutes}m', '--quiet']
        try:
            result = subprocess.run(commands, capture_output=True, text=True, check=True, timeout=5)
            result.check_returncode()
        except subprocess.TimeoutExpired as e:
            logger.error("Token generation timed out. Is cells running?")
            raise RuntimeError("Token generation timed out. Is cells running?") from e
        except subprocess.CalledProcessError as e:
            logger.error("Failed to generate token.")
            raise RuntimeError("Failed to generate token.") from e

        token = result.stdout.strip()
        if not token:
            raise RuntimeError("Generated token is empty.")
        
        return token

    def _configure_cells_client(self):
        commands = ['cec', 'configure', 'token', '--url', self._url, '--login', self._user, '--token', self.token(self._user)]
        try:
            output = subprocess.run(commands, capture_output=True, text=True, check=True)
            output.check_returncode()
        except subprocess.CalledProcessError as e:
            logger.error("Failed to configure Cells Client.")
            raise RuntimeError("Failed to configure Cells Client.") from e

    def update_tag(self, node_id: str, tag: str):
        try:
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.token('admin')}"
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
            response = requests.put(endpoint, headers=headers, data=payload)
            response.raise_for_status()
            logger.debug(f"Tag: {tag} updated for node: {node_id}")
        except Exception as e:
            err_msg = f"Unexpected error: {e}"
            logger.error(err_msg)
            raise RuntimeError(err_msg) from e

    def update_tag(self, node_id: str, tag: str, dip: bool = False):
        try:
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.token('admin')}"
            }
            payload = json.dumps({
                "MetaDatas": [
                    {
                        "JsonValue": f"\"{tag}\"",
                        "Namespace": f"{'usermeta-dip-progress' if dip else 'usermeta-a3m-progress'}",
                        "NodeUuid": node_id
                    }
                ],
                "Operation": "PUT"
            })
            endpoint = f'{self._url}/a/user-meta/update'
            response = requests.put(endpoint, headers=headers, data=payload)
            response.raise_for_status()
            logger.info(f"{'usermeta-dip-progress' if dip else 'usermeta-a3m-progress'}: {tag} updated for node: {node_id}")
        except Exception as e:
            err_msg = f"Unexpected error: {e}"
            logger.error(err_msg)
            raise RuntimeError(err_msg) from e

    def gather_child_nodes(self, parent_curate_node_path: str) -> list:
        logger.info(f"Gathering children of {parent_curate_node_path}")
        try:
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {self.token('admin')}"
            }
            payload = json.dumps({
                "Node": {
                    "Path": str(parent_curate_node_path)
                },
                "Recursive": True
            })
            endpoint = f"{self._url}/a/tree/admin/list"
            response = requests.post(endpoint, headers=headers, data=payload)
            response.raise_for_status()
        
        except Exception as e:
            err_msg = f"Unexpected error: {e}"
            logger.error(err_msg)
            raise RuntimeError(err_msg) from e
        return response.json().get('Children', [])

    def download_node(self, destination_path: Path, node_path: Path) -> Path:
        destination_path.mkdir(parents=True, exist_ok=True)
        commands = ['cec', 'scp', f'cells:///{str(node_path)}', str(destination_path)]
        subprocess.run(commands, capture_output=True, text=True, check=True)
        
        downloaded_files = list(destination_path.iterdir())
        if len(downloaded_files) == 1:
            return Path(downloaded_files[0])
        else:
            raise ValueError("Expected a single file or folder to be downloaded.")

    def upload_node(self, file_path: Path, curate_destination: str) -> Path:
        commands = ['cec', 'scp', str(file_path), f'cells://{curate_destination}/']
        subprocess.run(commands, capture_output=True, text=True, check=True)
        return Path(curate_destination) / file_path.name
