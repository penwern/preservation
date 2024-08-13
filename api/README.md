# Curate Preservation API

The Curate Preservation API is intended to be used alongside [Preservation Workflow](preservation/README.md) to integrate storing and managing preservation configurations and AtoM credentials.

## Usage
The preservation API is a FastAPI application that can be run locally.
```
# As pydio user

python -m uvicorn api:app --host 0.0.0.0 --port 8000 --root-path /api
```

It's recommened that the preservation API is run as a service.

The service can be found in the templates directory.
```
# As root

cp templates/curate_api.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now curate_api
```

## OpenAPI Documentation
The OpenAPI documentation can be found at http://localhost:8000/docs
When using a proxy, the OpenAPI documentation can be found at http://curate.example.com/api/docs

## Proxy
It's recommended that the preservation API is proxied through a reverse proxy.

An example nginx configuration is provided in the templates directory.

```
# As root

cp templates/curate_proxy /etc/nginx/sites-available/
ln -s /etc/nginx/sites-available/curate_proxy /etc/nginx/sites-enabled/curate_proxy
systemctl reload nginx
```

