# Curate Preservation Scripts

See [Preservation Workflow README](preservation/README.md) for Curate Preservation configurations.

See [Preservation API README](api/README.md) for Curate Preservation configurations.

## Requirements
- Ubuntu
- Python3

## Tested
- Ubuntu 20.04
- Python 3.9.19

## Logs
Logs are expected to be located in /var/cells/penwern/logs.
A template logrotate configuration is provided in the templates directory.
```
# As root

cp templates/preservation_logrotate /etc/logrotate.d/preservation_logs
```
