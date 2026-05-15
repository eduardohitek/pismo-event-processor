#!/bin/bash
set -e
docker compose run --rm terraform sh -c "terraform init && terraform apply -auto-approve"
