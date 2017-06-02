from models import Client
from extensions import db


class TestClient(object):

    def test_create(self):
        c = Client.create('comp_id', 'comp_name')
        assert False