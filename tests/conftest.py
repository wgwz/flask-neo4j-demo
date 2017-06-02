import pytest
from factory import create_app

@pytest.fixture(scope='session')
def app():
    _app = create_app({
        'SERVER_NAME': 'testingapplication',
        'TESTING': True,
        'PY2NEO_HOST': 'neo4jtest',
        'PY2NEO_HTTP_PORT': 7475,
        'PY2NEO_BOLT_PORT': 7688
    })
    with _app.app_context():
        yield app

@pytest.fixture(scope='session')
def client(app):
    yield app.test_client()