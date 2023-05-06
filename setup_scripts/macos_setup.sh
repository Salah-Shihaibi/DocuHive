brew upgrade
brew update && brew install poppler qpdf tesseract rabbitmq
brew services start rabbitmq
pip3 install poetry
poetry config virtualenvs.create false && poetry lock --check && poetry install --no-dev --no-interaction --only main
export PYTHONUNBUFFERED=True