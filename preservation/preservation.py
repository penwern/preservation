import argparse
import json
import logging
import shutil
import subprocess
import sys
import zipfile
import xml.etree.ElementTree as ET
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from py7zr import SevenZipFile

import settings
from curate import CurateManager
from a3m import A3MManager
from database import DatabaseManager


logger = logging.getLogger("preservation")
logger.setLevel(logging.DEBUG)
logger.handlers.clear() # Remove existing handlers to prevent duplicate logging
file_handler = logging.FileHandler(f'{settings.LOG_DIRECTORY}/preservation.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.propagate = False # Don't maintain logger

namespaces = {
    'premis': 'http://www.loc.gov/premis/v3',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}
for prefix, uri in namespaces.items():
    ET.register_namespace(prefix, uri)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Curate Preservation')
    parser.add_argument('-c', '--config_id', help='Config ID', type=int, required=True)
    parser.add_argument('-n', '--nodes', help='Array of node submitted from Curate', required=True)
    parser.add_argument('-u', '--user', help='User', required=True)
    args = parser.parse_args()
    return args

class Package():
    def __init__(self, node_json: dict, curate_prefix: str = None):
        """
        - Expects curate node data.
        - Builds dc and isadg metadata json.
        - Builds premis object element and event elements.
        """
        self.uuid = node_json['Uuid']
        
        self.is_dir = True if node_json['Type'] in ('COLLECTION', 2) else False
        self.mime_type = self._parse_mime_type(node_json['MetaStore'].get('mime', None))
        
        self.curate_path = Path(node_json['Path'])
        self.curate_prefix = curate_prefix if curate_prefix else self.curate_path.parent
        self.local_path: Path = None # Updated as the package is processed
        
        relative_path = self.curate_path.relative_to(self.curate_prefix)
        self.object_path = f'objects/data/{relative_path}'
    
        # Metadata
        self.metadata = self._construct_metadata_json(node_json['MetaStore'])
        
        logger.info(f"Metadata: {self.metadata}")
        
        # Premis
        curate_premis_metadata = json.loads(node_json['MetaStore'].get('usermeta-premis-data', '{}'))
        self.premis_xml_object = None
        self.premis_xml_events_list = None
        if curate_premis_metadata:
            self.premis_xml_events_list = self._construct_premis_xml_events_list(curate_premis_metadata)
            if self.premis_xml_events_list:
                self.premis_xml_object = self._construct_premis_xml_object()
        self.children = []
        
        logger.debug(f"Package Created: {self}")
        
    def __str__(self) -> str:
        return f"{self.__class__.__name__}: {(vars(self))}"
        
    def _parse_mime_type(self, curate_node_mimetype: str) -> str:
        if curate_node_mimetype:
            return curate_node_mimetype.strip('"')
        else:
            return None
    
    def _construct_metadata_json(self, curate_metastore: dict) -> dict:
        """
        Expects Curate node MetaStore dict
        """
        metadata_json = {'filename': self.object_path}
        for key, value in curate_metastore.items():
            if key.startswith('usermeta-dc-'):
                metadata_json[f"dc.{key[len('usermeta-dc-'):]}"] = value
            if key.startswith('usermeta-isadg-'):
                metadata_json[f"isadg.{key[len('usermeta-isadg-'):]}"] = value
        return metadata_json if len(metadata_json) > 1 else {}

    def _construct_premis_xml_events_list(self, premis_metadata: dict) -> list: 
        """
        Construct premis xml element from json extracted from curate node MetaStore
        Note: If curate premis json closer follows premis structure we can convert directly from json to xml
        """
        pns = namespaces['premis']
        event_element_list = []
        for event_data in premis_metadata:
            # Event element
            event_element = ET.Element(f"{{{pns}}}event")
            
            # Event Identifier
            event_identifier = ET.SubElement(event_element, f"{{{pns}}}eventIdentifier")
            ET.SubElement(event_identifier, f"{{{pns}}}eventIdentifierType").text = event_data["event_identifier"]["event_identifier_type"]
            ET.SubElement(event_identifier, f"{{{pns}}}eventIdentifierValue").text = event_data["event_identifier"]["event_identifier_value"]
            
            # Event Type
            ET.SubElement(event_element, f"{{{pns}}}eventType").text = event_data["event_type"]
            
            # Event Date Time
            ET.SubElement(event_element, f"{{{pns}}}eventDateTime").text = event_data["event_date_time"]
            
            # Event Detail Information
            event_detail_information = ET.SubElement(event_element, f"{{{pns}}}eventDetailInformation")
            ET.SubElement(event_detail_information, f"{{{pns}}}eventDetail").text = event_data["event_detail_information"]["event_detail"]
            
            # Event Outcome Information
            event_outcome_information = ET.SubElement(event_element, f"{{{pns}}}eventOutcomeInformation")
            ET.SubElement(event_outcome_information, f"{{{pns}}}eventOutcome").text = str(event_data["event_outcome_information"]["event_outcome"]) # Ensure string - fixes issue with writing siegfried output
            event_outcome_detail = ET.SubElement(event_outcome_information, f"{{{pns}}}eventOutcomeDetail")
            ET.SubElement(event_outcome_detail, f"{{{pns}}}eventOutcomeDetailNote").text = event_data["event_outcome_information"]["event_outcome_detail"]["event_outcome_detail_note"]

            event_element_list.append(event_element)
        return event_element_list
    
    def _construct_premis_xml_object(self) -> ET.Element:
        """
        Builds the premis object element with identifiers linking to each event element
        """
        """
        Each object will have multiple events.
        Each object links to its relevant events through objects linkingEventIdentifier
        Agent must be made for curate agents: User, Org, Software
        Each event must link to agents
        """
        
        pns = namespaces['premis']
        
        #   Object element
        object_elem = ET.Element(f"{{{pns}}}object")
        object_elem.set(f"{{{namespaces['xsi']}}}type", "premis:file")
        #       Object identifier element
        object_identifier_elem = ET.SubElement(object_elem, f"{{{pns}}}objectIdentifier")
        object_identifier_type_elem = ET.SubElement(object_identifier_elem, f"{{{pns}}}objectIdentifierType")
        object_identifier_type_elem.text = "UUID"
        object_identifier_value_elem = ET.SubElement(object_identifier_elem, f"{{{pns}}}objectIdentifierValue")
        object_identifier_value_elem.text = str(uuid4())
        
        #       Object characteristics element
        object_characteristics_elem = ET.SubElement(object_elem, f"{{{pns}}}objectCharacteristics")
        format_elem = ET.SubElement(object_characteristics_elem, f"{{{pns}}}format")
        format_designignation_elem = ET.SubElement(format_elem, f"{{{pns}}}formatDesignation")
        format_name_elem = ET.SubElement(format_designignation_elem, f"{{{pns}}}formatName")
        format_name_elem.text = self.mime_type
        
        #       Original name element
        original_name_elem = ET.SubElement(object_elem, f"{{{pns}}}originalName")
        original_name_elem.text = self.object_path
        
        #       Linking event identifier elements
        for event in self.premis_xml_events_list:
            linking_event_identifier_elem = ET.SubElement(object_elem, f"{{{pns}}}linkingEventIdentifier")
            event_identifiter_elem = event.find(f"{{{pns}}}eventIdentifier")
            linking_event_identifier_type_elem = ET.SubElement(linking_event_identifier_elem, f"{{{pns}}}linkingEventIdentifierType")
            linking_event_identifier_type_elem.text = event_identifiter_elem.find(f"{{{pns}}}eventIdentifierType").text
            linking_event_identifier_value_elem = ET.SubElement(linking_event_identifier_elem, f"{{{pns}}}linkingEventIdentifierValue")
            linking_event_identifier_value_elem.text = event_identifiter_elem.find(f"{{{pns}}}eventIdentifierValue").text

        return object_elem
        
    def get_curate_alt_path(self) -> Path:
        # Adapt node path for Cells Client
        curate_path_parts = self.curate_path.parts
        workspace_name = curate_path_parts[0]
        node_path_parts = curate_path_parts[2:] if workspace_name == 'personal' else curate_path_parts[1:]
        node_path = Path(settings.WORKSPACE_MAPPING[workspace_name], *node_path_parts)
        return node_path
    
    def write_metadata_json(self, destination: Path):
        """
        Writes DC and ISAD(G) json to package metadata directory.
        """
        full_metadata_list = [self.metadata] if self.metadata else []
        for child in self.children:
            if child.metadata:
                full_metadata_list.append(child.metadata)
                
        metadata_file_path = destination / 'metadata.json'
        if full_metadata_list:
            with open(metadata_file_path, 'w') as metadata_file:
                json.dump(full_metadata_list, metadata_file, indent=4)
            logger.debug(json.dumps(full_metadata_list, indent=4))
        
    def write_premis_xml(self, destination: Path, premis_agents: list):
        """
        Write premis xml to package metadata directory
        """
        
        # Root premis element
        premis_elem = ET.Element(f"{{{namespaces['premis']}}}premis", version="3.0")
        premis_elem.set(f"{{{namespaces['xsi']}}}schemaLocation", "http://www.loc.gov/premis/v3 https://www.loc.gov/standards/premis/premis.xsd")
        
        # Attach object elements
        for package in [self] + self.children:
            if package.premis_xml_object:
                premis_elem.append(package.premis_xml_object)
        
        # Attach event elements
        for package in [self] + self.children:
            if package.premis_xml_events_list:
                for event in package.premis_xml_events_list:
                    # Attach agents
                    for agent in premis_agents:
                        linking_agent_identifier = ET.SubElement(event, f"{{{namespaces['premis']}}}linkingAgentIdentifier")
                        ET.SubElement(linking_agent_identifier, f"{{{namespaces['premis']}}}linkingAgentIdentifierType").text = agent['identifier']['type']
                        ET.SubElement(linking_agent_identifier, f"{{{namespaces['premis']}}}linkingAgentIdentifierValue").text = agent['identifier']['value']
                    premis_elem.append(event)
        
        # Premis Agents
        for agent in premis_agents:
            agent_elem = ET.SubElement(premis_elem, "premis:agent")
            agent_identifier_elem = ET.SubElement(agent_elem, "premis:agentIdentifier")
            ET.SubElement(agent_elem, "premis:agentName").text = agent['name']
            ET.SubElement(agent_elem, "premis:agentType").text = agent['type']
            ET.SubElement(agent_identifier_elem, "premis:agentIdentifierType").text = agent['identifier']['type']
            ET.SubElement(agent_identifier_elem, "premis:agentIdentifierValue").text = agent['identifier']['value']
        
        logger.debug(ET.tostring(premis_elem, encoding='UTF-8', xml_declaration=True).decode())
        
        if premis_elem.find(f"{{{namespaces['premis']}}}object"):
            premis_file_path = destination / 'premis.xml'

            with open(premis_file_path , 'wb') as premis_file:
                tree = ET.ElementTree(premis_elem)
                tree.write(premis_file, encoding="UTF-8", xml_declaration=True)
            
            logger.info(f"Wrote premis xml to {premis_file_path}")
            logger.debug(f"Summary: XML Contains {len(list(premis_elem))} elements.")
            
    def update_local_path(self, new_local_path: Path):
        """
        Ensure new local path exists and updates the local path of the package.
        """
        if new_local_path.exists():
            self.local_path = new_local_path
            print(f"Updated local path to {self.local_path}")
        else:
            raise FileExistsError(f"New local path {new_local_path} does not exist.")


class Preservation():
    def __init__(self, config_id: int, user: str):
        """
        Initates components required for Curate preservation procedure
        """
        self.config_id = config_id
        self.user = user
        self.processing_directory = Path(settings.PROCESSING_DIRECTORY)
        self.processing_directory.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created processing directory {self.processing_directory}")
        
        self.db_manager = DatabaseManager()
        
        # Get preservation and a3m config from database
        self.processing_config, a3m_config = self.db_manager.get_preservation_processing_configs(self.config_id)
        
        self.curate_manager = CurateManager(self.user, settings.CURATE_URL)
        self.a3m_manager = A3MManager(a3m_config, self.processing_directory, settings.A3M_VERSION)
        
        self.premis_agents = [
            {
                'name': 'Curate',
                'type': 'Software',
                'identifier': {
                    'type': 'Preservation System',
                    'value': f'Curate Version={settings.CURATE_VERSION}'
                }
            },
            {
                'name': 'Penwern Limited',
                'type': 'Organization',
                'identifier': {
                    'type': 'Organization Name',
                    'value': 'Penwern Limited'
                }
            },
            {
                'name': 'Curate User',
                'type': 'User',
                'identifier': {
                    'type': 'Curate User Name',
                    'value': self.user
                }
            }
        ]
        
    def _extract_7z(self, archive_path: Path) -> Path:
        """
        Extract 7z archive to same directory.
        """
        target_folder = archive_path.parent

        command = ['7z', 'x', str(archive_path), '-o' + str(target_folder)]
        try:
            subprocess.run(command, check=True)
            logger.debug("Extraction completed successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"An error occurred during extraction: {e}")
            raise

        return target_folder / archive_path.stem

        
    def get_new_processing_directory(self) -> Path:
        """
        Returns processing directory with a new UUID directory.
        """
        new_dir = self.processing_directory / str(uuid4())
        new_dir.mkdir()
        logger.debug(f"Created new processing directory {new_dir}")
        return new_dir
    
    def download_package(self, package: Package, download_path_prefix: Path) -> Path:
        """
        Downloads the package.
        Returns the download path.
        """
        download_path = download_path_prefix / 'curate_download'
        downloaded_path = self.curate_manager.download_node(download_path, package.get_curate_alt_path())
        return downloaded_path
    
    def prepare_package_for_transfer(self, package: Package, processing_directory: Path) -> Path:
        """
        Transforms the submitted package into archive transfer state.
        - Creates transfer directory
        - Populates data directory
        - Writes metadata to metadata directory
        Returns path to transfer directory.
        """
        transfer_directory = processing_directory / 'transfer'
        transfer_data_path = transfer_directory / 'data'
        transfer_data_path.mkdir(parents=True)

        if zipfile.is_zipfile(package.local_path):
            with zipfile.ZipFile(package.local_path, 'r') as zip_ref:
                zip_ref.extractall(transfer_data_path / package.local_path.stem)
            logger.debug(f"Extracted {package.local_path} to {transfer_data_path / package.local_path.stem}.")
        elif package.local_path.is_file():
            shutil.copy(package.local_path, transfer_data_path)
            logger.debug(f"Copied file {package.local_path} to {transfer_data_path}.")
        elif package.local_path.is_dir():
            shutil.copytree(package.local_path, transfer_data_path / package.local_path.stem)
            logger.debug(f"Copied folder {package.local_path} to {transfer_data_path / package.local_path.stem}.")
        else:
            raise RuntimeError('Node is in a format we cannot handle.')
        
        logger.info(f"Created transfer directory {transfer_directory}.")
        
        transfer_metadata_path = transfer_directory / 'metadata'
        transfer_metadata_path.mkdir()
        logger.info("Writing json metadata")
        package.write_metadata_json(transfer_metadata_path)
        logger.info("Writing Premis")
        package.write_premis_xml(transfer_metadata_path, self.premis_agents)
        
        return transfer_directory
    
    def execute_transfer(self, package: Package, processing_directoy: Package) -> Path:
        """
        Executes the a3m transfer.
        Moves compressed AIP to shared volume.
        Extracts 7z AIP.
        Returns AIP Path.
        """
        aip_name = self.a3m_manager.execute_a3m_transfer(package.local_path, package.curate_path.stem.strip().replace(' ', ''))
        expected_container_aip_path = Path("/home/a3m/.local/share/a3m/share/completed/" + aip_name)
        logger.info(f'Successfully created AIP in container {expected_container_aip_path}')
        
        # Move AIP to Shared Volume
        package_aip_directoy = processing_directoy / 'aip'
        package_aip_directoy.mkdir()
        aip_path = self.a3m_manager.move_file_in_container(expected_container_aip_path, package_aip_directoy)
        logger.info(f'Moved AIP to shared volume {aip_path}')

        # Extract 7z aip - we do this here as it allows us to compressing using user specified compression algorithm
        # Could do 'if compress and compression algo not 7z'
        extracted_aip_path = self._extract_7z(aip_path)
        logger.info(f'Extracted AIP to {extracted_aip_path}')
        return extracted_aip_path
    
    def compress_package(self, package: Package) -> Path:
        """
        Compresses the package.
        """
        zip_path = package.local_path.with_suffix('.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in package.local_path.rglob('*'):
                arcname = file_path.relative_to(package.local_path)
                zipf.write(file_path, arcname=arcname)
        logger.info(f"Compressed {package.local_path} to {zip_path}.")
        return zip_path
        
    def upload_package(self, package: Package):
        curate_destination = 'archive'
        self.curate_manager.upload_node(package.local_path, curate_destination)
        logger.info(f"Uploaded {package.local_path} to Curate {curate_destination}.")


def process_node(preserver: Preservation, node: dict, processing_directory: Path):
    
    try:
        logger.info(f"Processing node {node['Path']}")
        
        # Populate main package
        package = Package(node)
        
        # Populate child packages of directory packages
        if package.is_dir:
            for child_node in preserver.curate_manager.gather_child_nodes(package.curate_path):
                package.children.append(Package(child_node, package.curate_prefix))
        
        # Download the package
        preserver.curate_manager.update_tag(package.uuid, 'Processing package...')
        downloaded_path = preserver.download_package(package, processing_directory)
        package.update_local_path(downloaded_path)
        
        # Manipulate package to transfer state
        preserver.curate_manager.update_tag(package.uuid, 'Preparing package...')
        transfer_directory = preserver.prepare_package_for_transfer(package, processing_directory)
        package.update_local_path(transfer_directory)
        
        # Execute A3M transfer on package
        preserver.curate_manager.update_tag(package.uuid, 'Submitting package...')
        aip_path = preserver.execute_transfer(package, processing_directory)
        package.update_local_path(aip_path)
        
        # Compress AIP if enabled in processing config
        if preserver.processing_config['compress_aip']:
            preserver.curate_manager.update_tag(package.uuid, 'Compressing AIP...')
            extracted_path = preserver.compress_package(package)
            package.update_local_path(extracted_path)
        
        # Upload to Curate
        preserver.curate_manager.update_tag(package.uuid, 'Uploading AIP...')
        preserver.upload_package(package)
    except Exception as e:
        logger.error(e)
        preserver.curate_manager.update_tag(package.uuid, 'Preservation Failed - Try Again')
        # shutil.rmtree(processing_directory)
        raise e
    preserver.curate_manager.update_tag(package.uuid, '🔒 Preserved')
    
def main():

    logger.debug(f"Arguments: {sys.argv}")
    try:
        args = parse_arguments()
    except Exception as e:
        logger.error(e)
        raise
    logger.debug(f"Node Type: {type(args.nodes)}") 
    try:
        preserver = Preservation(config_id = args.config_id, user=args.user)
    except Exception as e:
        logger.error(e)
        raise
    for node in json.loads(args.nodes):
        try:
            processing_directory = preserver.get_new_processing_directory()
            process_node(preserver, node, processing_directory)
        except Exception:
            continue

if __name__ == '__main__':
    logger.info(' =============== NEW =============== ')
    try:
        main()
    except Exception as e:
        logger.error(e)
        raise
    logger.info(' ============= COMPLETE ============= \n')

