#!/bin/bash
# Provisions LocalStack resources using the setup service (AWS CLI).
# Equivalent to: make up
set -e
docker compose up setup
