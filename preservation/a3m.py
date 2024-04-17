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
    def __init__(self, config: dict, processing_directory: Path, a3m_version: str):
        self.processing_config = config
        self.a3m_version = a3m_version
        self.daemon = self._initiate_daemon(processing_directory)

    def _construct_processing_config_string(self) -> str:
        config_string = ""
        for k, v in self.processing_config.items():
            config_string += f"--processing-config {k}={v} "
        return config_string
    
    def _initiate_daemon(self, volume: Path):
        """
        Ensure docker network a3m_network exists.
        Create processing volume.
        Ensure docker container a3md exists.
        """
        
        # Create network if it doesn't exist
        try:
            docker_client.networks.get("a3m-network")
            logger.info('Network a3m-network already started')
        except docker.errors.NotFound:
            docker_client.networks.create("a3m-network")
            logger.info('Created docker network a3m-network')

        # Start container if it doesn't exist
        try:
            a3md_container = docker_client.containers.get("a3md")
            logger.info('Container a3md already started')
        except docker.errors.NotFound:
            logger.info('Starting docker container a3md')
            a3md_container = docker_client.containers.run(
                f"ghcr.io/artefactual-labs/a3m:{self.a3m_version}",
                name="a3md",
                user=1000,
                volumes=[f"{str(volume)}:{str(volume)}"],
                detach=True,
                remove=True,
                network="a3m-network",
                ports={"7000/tcp": 7000},
                environment=["A3M_DEBUG=yes"],
            )
            self._wait_for_deamon_start(a3md_container)
            
            logger.info('Successfully started docker container a3md')
            logger.debug(f'Container logs: {a3md_container.logs()}')
        return a3md_container

    def _wait_for_deamon_start(self, daemon):
        """
        Wait for the a3m daemon to become healthy within a timeout period.
        """
        timeout = 30
        start_time = time.time()
        while time.time() - start_time < timeout:
            daemon.reload()
            logger.debug(f"Container status: {daemon.status}")
            if daemon.status == "running":
                return
            time.sleep(5)
        raise TimeoutError(f"Container {daemon.name} did not become healthy within {timeout} seconds")
    
    def _sanitize_container_name(self, input_string: str) -> str:
        """
        Convert input string to fit docker container name format.
        Adds UUID.
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
        logger.info(f'Creating Container: {container_name}')
        logger.info(f'Starting A3M transfer {transfer_path}')
        logger.debug(f'Commands {commands}')
        container = docker_client.containers.run(
            f"ghcr.io/artefactual-labs/a3m:{self.a3m_version}",
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

        aip_name = f"{transfer_name}-{aip_uuid}.7z"
        return aip_name
    
    def move_file_in_container(self, src_path: Path, dst_path: Path) -> Path:
        """
        Move a file within the running a3m daemon.
        """
        exec_result = self.daemon.exec_run(f'mv "{src_path}" "{dst_path}"', user="root")
        new_path = dst_path / src_path.name

        if exec_result.exit_code != 0:
            logger.debug(exec_result)
            raise RuntimeError(f"Failed to move the file within the container: {exec_result.output}")
        return new_path
