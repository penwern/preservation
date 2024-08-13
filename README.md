# Curate Preservation Scripts

## Requirements
- Python3
- Docker
- py7zip-full
- RSync
- Pydio Cells
- Pydio Cells Client

## Tested
- Ubuntu 20.04
- Python 3.9.19
- Docker 27.1.1
- Cells 4.4.1
- Cells Client 4.1.0
- A3M 0.7.9
---
---

# Preservation
## Requirements
### A3MD
A3M Daemon docker container must be running before preservation can be started.
```
# Bash Script
chmod +x start_a3md_container.sh
./start_a3md_container.sh
```

```
# Docker
docker run -d --name a3md --user 1000 -v /tmp/curate/preservation:/tmp/curate/preservation --network a3m-network -p 7000:7000 --restart unless-stopped ghcr.io/artefactual-labs/a3m:v0.7.9
```

It's recommened that the a3m daemon is run as a systemd service.
```
cp templates/a3md.service /etc/systemd/system/a3md.service
systemctl daemon-reload
systemctl start a3md.service
systemctl enable a3md.service
```
### Pydio Cells
Pydio Cells must be running before preservation can be started.

### Pydio Cells Client
Pydio Cells Client must be installed and configured before preservation can be started.

## Run Preservation Workflow
```
python -m main.py -u {user} -c {preservation config id} -n {array of curate node json}
```

# Preservation API
```
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --root-path /api
```
