#!/bin/bash

sudo apt upgrade -y
sudo apt update && sudo apt install -y  poppler-utils qpdf tesseract-ocr python3 python3-pip

sudo sh -c 'echo "deb http://www.rabbitmq.com/debian/ testing main" >> /etc/apt/sources.list'
sudo curl -fsSL https://www.rabbitmq.com/rabbitmq-release-signing-key.asc | sudo apt-key add -
sudo apt update
sudo apt install -y rabbitmq-server

sudo service rabbitmq-server start
sudo pip install poetry

sudo poetry config virtualenvs.create false && sudo poetry lock --check && sudo poetry install --no-dev --no-interaction --only main
export PYTHONUNBUFFERED=True
sudo pip uninstall -y poetry
