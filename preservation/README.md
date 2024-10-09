# Curate Preservation Workflow

The Curate Preservation Workflow is intended to be used alongside [Pydio Cells](https://pydio.com/en/pydio-cells/overview) to integrate complitant preservation workflows into the platform.

Preservation configurations are stored in the database and can be managed through the [Preservation API](api/README.md).

## Requirements
- Python3
- Docker
- py7zip-full
- RSync
- Pydio Cells
- Pydio Cells Client

## Tested
- Docker 27.1.1
- Cells 4.4.1
- Cells Client 4.1.0
- A3M 0.7.9

## Requirements
### A3MD
A3M Daemon docker container must be running before preservation can be started.
```
# As pydio user

# Run the bash script
chmod +x start_a3md_container.sh
./start_a3md_container.sh
```

It's recommened that the a3m daemon start script is run at startup.
```
# As pydio user
crontab -e

# Add the following line to the end of the file
@reboot /var/cells/penwern/services/preservation/start_a3md_container.sh
```
### Pydio Cells
Pydio Cells must be running before preservation can be started.

### Pydio Cells Client
Pydio Cells Client must be installed and configured before preservation can be started.

### Penwern Curate Preservation API
[Penwern Curate Preservation API](api/README.md) should be run at least once before preservation can be started to create the db file.

## Python Requirements
```
# As pydio user

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage
```
# As pydio user

python main.py -u {user} -c {preservation config id} -n {[curate nodes]}
```
