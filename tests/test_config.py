from extensions import db

class TestPytestConfig(object):

    def test_db_connection(self, app):
        assert False