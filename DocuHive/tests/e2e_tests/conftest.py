import pytest
from DocuHive.tests.DocuHive_client.client import DocuHiveClient


@pytest.fixture(scope="session", autouse=True)
def setup_before_tests():
    DocuHiveClient().empty_database()
