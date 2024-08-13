import os
import re
from uuid import uuid4
import docker
import logging
import time
import re
from pathlib import Path

docker_client = docker.from_env()
logger = logging.getLogger("preservation")

class A3MManager:
    def __init__(self, config: dict, a3m_docker_image: str):
        self.processing_config = config
        self.a3m_docker_image = a3m_docker_image
        self.daemon = self._a3md_checks()

    def _construct_processing_config_string(self) -> str:
        config_string = ""
        for k, v in self.processing_config.items():
            config_string += f"--processing-config {k}={v} "
        return config_string
    
    def _a3md_checks(self):
        """
        Ensures docker network a3m_network exists.
        Ensures docker container a3md exists.
        """
        try:
            docker_client.networks.get("a3m-network")
            logger.debug('A3M network found')
        except docker.errors.NotFound as e:
            logger.error("A3M network not found.")
            raise RuntimeError("A3M network not found.") from e

        try:
            a3md_container = docker_client.containers.get("a3md")
            logger.debug('A3M Daemon container found')
        except docker.errors.NotFound as e:
            logger.error("A3M Daemon container not found.")
            raise RuntimeError("A3M Daemon container not found.") from e
        return a3md_container

    def _sanitize_container_name(self, input_string: str) -> str:
        """
        Sanitize input string to fit docker container name format.
        Characters not allowed are replaced with underscores.
        Appends UUID for uniqueness.
        """
        sanitized_string = re.sub(r'[^a-zA-Z0-9_.-]', '_', input_string)
        
        if not re.match(r'^[a-zA-Z0-9]', sanitized_string):
            sanitized_string = '_' + sanitized_string
        
        if not re.match(r'[a-zA-Z0-9]$', sanitized_string):
            sanitized_string += '_'
        
        return sanitized_string + str(uuid4())

    def execute_a3m_transfer(self, transfer_path: Path, transfer_name: str) -> str:
        """
        Execute an A3M transfer.
        """
        commands = [
            "-m", "a3m.cli.client",
            "--address=a3md:7000",
            "--no-input",
            "--name", transfer_name,
            str(transfer_path)
        ]
        for k, v in self.processing_config.items():
            commands.append("--processing-config")
            commands.append(f"{k}={v}")
        container_name = self._sanitize_container_name(transfer_name)
        logger.debug(f'Creating Container {container_name}')
        logger.debug(f'Starting A3M transfer {transfer_path}')
        container = docker_client.containers.run(
            self.a3m_docker_image,
            name=container_name,
            detach=True,
            network="a3m-network",
            entrypoint="python",
            command=commands,
            environment=["A3M_DEBUG=yes"],
        )
        exit_status = container.wait()
        container_logs = container.logs().decode('utf-8')
        container.remove()

        if exit_status['StatusCode'] != 0:
            err_msg = f"Transfer failed with exit code: {exit_status['StatusCode']}"
            logger.debug(f"Container logs: {container_logs}")
            raise RuntimeError(err_msg)

        # Flip the log so we get last uuid (when debugging)
        reversed_logs = ' '.join(container_logs.split('\n')[::-1])
        aip_uuid = re.search(
            r"\b[A-Fa-f0-9]{8}(?:-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}\b",
            reversed_logs
        )[0]
        if aip_uuid is None:
            raise RuntimeError("Could not find AIP UUID")
        logger.debug(f"AIP UUID: {aip_uuid}")
        return aip_uuid
    
    def move_file_in_container(self, src_path: Path, dst_path: Path) -> Path:
        """
        Move a file within the running a3m daemon.
        """
        mv_exec_result = self.daemon.exec_run(f'mv "{src_path}" "{dst_path}"', user="root")
        new_path = dst_path / src_path.name

        if mv_exec_result.exit_code != 0:
            logger.debug(mv_exec_result)
            raise RuntimeError(f"Failed to move the file within the container: {mv_exec_result.output}")
        
        # Change ownership of the file to the current user
        chown_exec_result = self.daemon.exec_run(f'chown -R {os.getuid()}:{os.getgid()} "{new_path}"', user="root")

        if chown_exec_result.exit_code != 0:
            logger.debug(chown_exec_result)
            raise RuntimeError(f"Failed to move the file within the container: {mv_exec_result.output}")
        
        return new_path
