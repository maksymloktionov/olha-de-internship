#!/bin/bash


docker compose up -d --build

docker exec -it python-postgres-app bash