# db used in class level teardown
# bc using the db-fixture there did not seem to work
# https://docs.pytest.org/en/latest/xunit_setup.html#class-level-setup-teardown
from extensions import db 

from models import Client, Onboard, BuildClient
from models import GenericProcess, GenericStep, BuildGeneric
from models import BuildClientOnboard


class TestClient(object):

    def test_create(self, db):

        NUM_PROPERTIES = 2
        COMPANY_ID = 'fake_company_id'
        COMPANY_NAME = 'fake_company_name'
        LABELS = ['Client', 'Person']

        client = Client.create(COMPANY_ID, COMPANY_NAME)

        cursor = db.graph.run((
                    "match (c:Client) "
                    "where c.company_id='%s' " 
                    "return c AS client" % COMPANY_ID
                ))
        cursor.forward()

        result = cursor.current()['client']

        assert result['company_id'] == COMPANY_ID
        assert result['company_name'] == COMPANY_NAME
        assert all([label in result.labels() for label in LABELS])
        assert len(result.viewkeys()) == NUM_PROPERTIES

        db.graph.run((
                    "match (c:Client) "
                    "where c.company_id='%s' " 
                    "delete c" % COMPANY_ID
                ))


class TestOnboard(object):

    def test_create(self, db):

        NUM_PROPERTIES = 2

        onboard = Onboard.create()

        cursor = db.graph.run((
                    "match (o:Onboard) "
                    "return o AS onboard"
                ))
        cursor.forward()

        result = cursor.current()['onboard']

        assert result['completed'] == False
        assert result['valid_onboard'] == False
        assert len(result.viewkeys()) == NUM_PROPERTIES

        db.graph.run((
                    "match (o:Onboard) "
                    "delete o"
                ))


class TestBuildClient(object):

    def test_initialize_new_client(self, db):

        COMPANY_ID = 'new_company_id'
        COMPANY_NAME = 'new_company_name'
        REL_TYPE = 'HAS_ONBOARD'

        build_client = BuildClient(COMPANY_ID, COMPANY_NAME)
        build_client.init_rels()

        cursor = db.graph.run((
                "match (c:Client)-[r:%s]->(o) "
                "where c.company_id='%s' "
                "return c, r, o" % (REL_TYPE, COMPANY_ID)
            ))
        cursor.forward()

        client = cursor.current()['c']
        has_onboard = cursor.current()['r']
        onboard = cursor.current()['o']

        assert client is not None
        assert onboard is not None
        assert REL_TYPE in has_onboard.types()

        db.graph.run((
                "match (c:Client)-[r:%s]->(o) "
                "where c.company_id='%s' "
                "delete c, r, o" % (REL_TYPE, COMPANY_ID)
            ))


class TestGenericProcess(object):

    def test_create(self, db):

        LABELS = ['GenericProcess']

        process = GenericProcess.create()

        cursor = db.graph.run((
            "match (p:GenericProcess) "
            "return p"
        ))
        cursor.forward()

        result = cursor.current()['p']
        assert all([label in result.labels() for label in LABELS])

        db.graph.run((
            "match (p:GenericProcess) "
            "delete p"
        ))


class TestGenericStep(object):

    def test_create(self, db):

        NUM_PROPERTIES = 3
        TASK_NAME = 'some-step-name'
        STEP_NUMBER = 201
        DURATION = 12

        step = GenericStep.create(TASK_NAME, STEP_NUMBER, DURATION)

        cursor = db.graph.run("match (s:GenericStep) return s")
        cursor.forward()

        result = cursor.current()['s']
        assert result['task_name'] == TASK_NAME
        assert result['step_number'] == STEP_NUMBER
        assert result['duration'] == DURATION
        assert len(result.viewkeys()) == NUM_PROPERTIES

        db.graph.run("match (s:GenericStep) delete s")


class TestBuildGeneric(object):

    @classmethod
    def setup_class(cls):
        cls.generic = BuildGeneric()
        cls.generic.init_steps()
        cls.generic.init_steps_rels()
        cls.NUM_STEPS = 5
        cls.STEPS_WITH_DEPENDS = {3, 4}
        cls.DEPENDENCIES = {3: set([0, 1, 2]), 4: set([0, 1, 2, 3])}

    @classmethod
    def teardown_class(cls):
        db.graph.run((
            "match (p:GenericProcess)-[:NEXT*]->(s) "
            "detach delete s, p"
        ))

    def test_next_rel(self, db):

        cursor = db.graph.run((
            "match (:GenericProcess)-[:NEXT*]->(s) "
            "return s order by s.step_number"
        ))
        num_results = 0
        for counter, result in enumerate(cursor):
            assert result['s']['step_number'] == counter
            num_results += 1

        assert num_results == self.NUM_STEPS 

    def test_has_step_rel(self, db):

        cursor = db.graph.run((
            "match (:GenericProcess)-[:HAS_STEP*]->(s) "
            "return s order by s.step_number"
        ))
        num_results = 0
        for counter, result in enumerate(cursor):
            assert result['s']['step_number'] == counter
            num_results += 1

        assert num_results == self.NUM_STEPS

    def test_first_step_rel(self, db):

        cursor = db.graph.run((
            "match (:GenericProcess)-[:FIRST_STEP]->(s) "
            "return s"
        ))
        cursor.forward()

        assert cursor.current()['s']['step_number'] == 0
        assert cursor.forward() == 0

    def test_last_step_rel(self, db):

        cursor = db.graph.run((
            "match (:GenericProcess)-[:LAST_STEP]->(s) "
            "return s"
        ))
        cursor.forward()

        assert cursor.current()['s']['step_number'] == self.NUM_STEPS - 1
        assert cursor.forward() == 0

    def test_get_steps_with_depends(self, db):

        cursor = db.graph.run((
            "match (:GenericProcess)-[:HAS_STEP*]->(s), "
            "(s)-[:DEPENDS_ON*]->(d) "
            "return collect(distinct s.step_number) AS steps"
        ))
        assert cursor.forward() == 1

        assert set(cursor.current()['steps']) == self.STEPS_WITH_DEPENDS

    def test_depends_on_rel(self, db):

        cursor = db.graph.run((
            "match (:GenericProcess)-[:HAS_STEP*]->(s), "
            "(s)-[:DEPENDS_ON*]->(d)"
            "return s , d order by s.step_number"
        ))

        results = {}
        for result in cursor:
            if results.get(result['s']['step_number']) is None:
                results[result['s']['step_number']] = set({}) 
            results[result['s']['step_number']].add(result['d']['step_number'])

        assert results == self.DEPENDENCIES


class TestBuildClientOnboard(object):

    @classmethod
    def setup_class(cls):

        cls.COMPANY_ID = 'some-id-for-company'
        cls.COMPANY_NAME = 'some-comp-name'

        cls.client = BuildClient(cls.COMPANY_ID, cls.COMPANY_NAME)
        cls.client.init_rels()
        
        cls.generic = BuildGeneric()
        cls.generic.init_steps()
        cls.generic.init_steps_rels()
        
        cls.client_onboard = BuildClientOnboard(cls.COMPANY_ID)
        cls.client_onboard.init_rels()

    @classmethod
    def teardown_class(cls):
        db.graph.run((
            "match (c:Client), (o:Onboard), (p:GenericProcess), (s:GenericStep) "
            "detach delete c, o, p, s"
        ))

    def test_basic(self):
        assert False