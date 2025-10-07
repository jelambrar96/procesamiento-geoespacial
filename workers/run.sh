#!/bin/bash

# reemplazar variables en .dlt/secrets.toml
envsubst < .dlt/secrets-template.toml > .dlt/secrets.toml

cat .dlt/secrets.toml

python3 pipeline_01_nyc_download.py
