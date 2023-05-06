# ITo start the BE run:
```shell
poetry install
poetry run python DocuHive/main.py
OMP_THREAD_LIMIT=1 poetry run celery -A  DocuHive.worker.celery worker --loglevel=info -Q cpu -n cpu -c 3
poetry run celery -A  DocuHive.worker.celery worker --loglevel=info -P gevent -Q io -n io -c 2
```

# Deployment commands
```shell
gunicorn DocuHive.main:app -b 0.0.0.0:8080 --workers=1 --threads=2 --worker-class=gevent --timeout 600
kill -9 $(lsof -t -i:8080) & ps auxww | grep 'celery worker' | awk '{print $2}' | xargs kill -9
gcloud compute scp --recurse DocuHive/ instance-1:Doc --zone us-central1-b
```

# Run tests
```
 PYTHONPATH=~/src/DocuHive-Backend poetry run pytest DocuHive/tests/e2e_tests
```

# To run scalene on tests
 cd into the test directory and run:
 ```shell
poetry run python -m scalene --- -m pytest DocuHive/tests/script_tests/test_cv_extractor.py
```

