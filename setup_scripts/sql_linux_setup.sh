#!/bin/bash

sudo apt update
sudo apt install -y libpq-dev gcc postgresql
sudo -u postgres bash << EOF
psql -c "CREATE USER psql WITH PASSWORD '0000';"
psql -c "CREATE DATABASE docuhive;"
psql -c "GRANT ALL PRIVILEGES ON DATABASE docuhive TO psql;"
EOF