import pytest
from factory import create_app
from extensions import db as _db


TEST_DB_URI = 'testdb'
TEST_DB_HTTP = 7475
TEST_DB_BOLT = 7688


@pytest.fixture(scope='session')
def app():
    _app = create_app({
        'SERVER_NAME': 'testingapplication',
        'TESTING': True,
        'PY2NEO_BOLT': None, # without this, creating a relationship off the OGM threw an error
        'PY2NEO_HOST': TEST_DB_URI,
        'PY2NEO_HTTP_PORT': TEST_DB_HTTP,
        'PY2NEO_BOLT_PORT': TEST_DB_BOLT
    })
    with _app.app_context():

        yield _app


@pytest.fixture(scope='session')
def db(app):

    assert app.config['PY2NEO_HOST'] == TEST_DB_URI
    assert TEST_DB_URI + ':' + str(TEST_DB_HTTP) in _db.graph.transaction_uri

    _db.graph.run("MATCH (n) DETACH DELETE n")

    cursor = _db.graph.run("MATCH (n) RETURN n")
    assert not cursor.forward()

    yield _db


@pytest.fixture(scope='session')
def client(app):

    yield app.test_client()