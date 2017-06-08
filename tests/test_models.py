# tests are written with reads via cypher
# the primary reason for this is that
# in production direct cypher queries are likely to be faster

from py2neo.types import Node

# _db used in class level teardown
# bc using the db-fixture there did not seem to work
# https://docs.pytest.org/en/latest/xunit_setup.html#class-level-setup-teardown
from extensions import db as _db

# broken into multiple lines to try to organize similar logic
# for the future this could be modularized further
from models import Client, Onboard, BuildClient
from models import GenericProcess, GenericStep, BuildGeneric
from models import BuildClientOnboard, UpdateClientOnboard
from models import Company, Employee, BuildEmployee
from models import Project, EmployeeInvolvement, EmployeeAccess
from models import Application, Database
from models import CrmDatabase, ErpDatabase, ComplianceDatabase
from models import EmployeeAppAccess


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
        _db.graph.run((
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
        _db.graph.run((
            "match (c:Client), (o:Onboard), (p:GenericProcess), (s:GenericStep) "
            "detach delete c, o, p, s"
        ))

    def test_must_follow_rel(self, db):

        cursor = db.graph.run((
            "match (c:Client)-[:HAS_ONBOARD]->()-[:MUST_FOLLOW]->(p) "
            "where c.company_id='%s' "
            "return c, p" % self.COMPANY_ID
        ))

        assert cursor.forward() == 1
        
        client = cursor.current()['c']
        process = cursor.current().get('p')

        assert client['company_id'] == self.COMPANY_ID
        assert process is not None

        assert cursor.forward() == 0


class TestUpdateClientOnboard(object):

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

        cls.REL_TYPE = 'HAS_COMPLETED'
        cls.STEPS_COMPLETED = [0, 4]
        cls.update_onboard = UpdateClientOnboard(cls.COMPANY_ID)

        for step in cls.STEPS_COMPLETED:
            cls.update_onboard.mark_step_complete(step)

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (c:Client), (o:Onboard), (p:GenericProcess), (s:GenericStep) "
            "detach delete c, o, p, s"
        ))

    def test_mark_step_complete(self, db):

        cursor = db.graph.run((
            "match ()-[r:HAS_COMPLETED]->(s) "
            "return r, s order by s.step_number"
        ))

        assert cursor.forward() == 1
        assert self.REL_TYPE in cursor.current()['r'].types()
        assert cursor.current()['s']['step_number'] == self.STEPS_COMPLETED[0]

        assert cursor.forward() == 1
        assert self.REL_TYPE in cursor.current()['r'].types()
        assert cursor.current()['s']['step_number'] == self.STEPS_COMPLETED[1]

        assert cursor.forward() == 0


class TestCompanyNode(object):

    @classmethod
    def setup_class(cls):

        cls.NAME = 'a-company-like-citi-bank'

        cls.company = Company()
        cls.company.push(cls.NAME)

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (c:Company) "
            "delete c"
        ))

    def test_company_node(self, db):

        cursor = db.graph.run((
            "match (c:Company) "
            "return c"
        ))

        assert cursor.forward() == 1
        assert cursor.current()['c']['name'] == self.NAME
        assert cursor.forward() == 0


class TestEmployeeNode(object):

    @classmethod
    def setup_class(cls):

        cls.ID = '1122452335'
        cls.EMAIL = 'gpwn@fuzz.org'
        cls.LABELS = {'Employee', 'Person'}

        cls.employee = Employee()
        cls.employee.push(cls.ID, cls.EMAIL)

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (e:Employee) "
            "delete e"
        ))

    def test_employee_node(self, db):

        cursor = db.graph.run((
            "match (e:Employee) "
            "return e"
        ))

        assert cursor.forward() == 1
        assert set(cursor.current()['e'].labels()) == self.LABELS
        assert cursor.current()['e']['id'] == self.ID
        assert cursor.current()['e']['email'] == self.EMAIL
        assert cursor.forward() == 0


class TestBuildEmployee(object):

    @classmethod
    def setup_class(cls):

        cls.COMPANY_NAME = 'a-company-like-citi-bank'
        cls.EMPLOYEE_ID = '1122452335'
        cls.EMPLOYEE_EMAIL = 'gpwn@fuzz.org'

        cls.build_employee = BuildEmployee(
            cls.EMPLOYEE_ID, 
            cls.EMPLOYEE_EMAIL, 
            cls.COMPANY_NAME
        )
        cls.build_employee.init_rels()

        cls.REL_TYPE = 'WORKS_FOR'

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (c:Company), (e:Employee) "
            "detach delete e, c"
        ))

    def test_build_employee(self, db):

        cursor = db.graph.run((
            "match (e:Employee)-[r:WORKS_FOR]->(c) "
            "return e, r, c"
        ))

        assert cursor.forward() == 1
        assert self.REL_TYPE in cursor.current()['r'].types()
        assert cursor.current()['e']['id'] == self.EMPLOYEE_ID
        assert cursor.current()['c']['name'] == self.COMPANY_NAME
        assert cursor.forward() == 0


class TestProjectNode(object):

    @classmethod
    def setup_class(cls):

        cls.LABELS = {'Project'}
        cls.project = Project()
        cls.project.create()

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (p:Project) "
            "delete p"
        ))

    def test_project_node(self, db):

        cursor = db.graph.run((
            "match (p:Project) "
            "return p"
        ))

        assert cursor.forward() == 1
        assert set(cursor.current()['p'].labels()) == self.LABELS
        assert cursor.forward() == 0


class TestEmployeeInvolvement(object):

    @classmethod
    def setup_class(cls):

        cls.CLIENT_ID = 'somecliid'
        cls.CLIENT_NAME = 'some-client-name-aka-company-name'

        cls.build_client = BuildClient(cls.CLIENT_ID, cls.CLIENT_NAME)
        cls.build_client.init_rels()

        cls.COMPANY_NAME = 'a-company-like-citi-bank'
        cls.EMPLOYEE_ID = '1122452335'
        cls.EMPLOYEE_EMAIL = 'gpwn@fuzz.org'

        cls.build_employee = BuildEmployee(
            cls.EMPLOYEE_ID, 
            cls.EMPLOYEE_EMAIL, 
            cls.COMPANY_NAME
        )
        cls.build_employee.init_rels()

        cls.employee_involve = EmployeeInvolvement(cls.EMPLOYEE_ID, cls.CLIENT_ID)
        cls.employee_involve.init_rels()

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (co:Company), (e:Employee), (cl:Client), (p:Project), (o:Onboard) "
            "detach delete co, e, cl, p, o"
        ))

    def test_employee_involvement(self, db):

        cursor = db.graph.run((
            "match (e:Employee)-[:WORKED_ON]->(p), (c)<-[:FOR_CLIENT]-(p)-[:FOR_ONBOARD]->(o) "
            "return p, c, o"
        ))

        assert cursor.forward() == 1
        assert isinstance(cursor.current().get('p'), Node)
        assert isinstance(cursor.current().get('c'), Node)
        assert isinstance(cursor.current().get('o'), Node)
        assert cursor.forward() == 0


class TestEmployeeAccess(object):

    @classmethod
    def setup_class(cls):

        cls.CLIENT_ID = 'somecliid'
        cls.CLIENT_NAME = 'some-client-name-aka-company-name'

        cls.build_client = BuildClient(cls.CLIENT_ID, cls.CLIENT_NAME)
        cls.build_client.init_rels()

        cls.build_generic = BuildGeneric()
        cls.build_generic.init_steps()
        cls.build_generic.init_steps_rels()

        cls.build_cli_onboard = BuildClientOnboard(cls.CLIENT_ID)
        cls.build_cli_onboard.init_rels()

        cls.COMPANY_NAME = 'a-company-like-citi-bank'
        cls.EMPLOYEE_ID = '1122452335'
        cls.EMPLOYEE_EMAIL = 'gpwn@fuzz.org'

        cls.build_employee = BuildEmployee(
            cls.EMPLOYEE_ID, 
            cls.EMPLOYEE_EMAIL, 
            cls.COMPANY_NAME
        )
        cls.build_employee.init_rels()

        cls.employee_involve = EmployeeInvolvement(cls.EMPLOYEE_ID, cls.CLIENT_ID)
        cls.employee_involve.init_rels()

        cls.STEP_ACCESSED = 2

        cls.employee_access = EmployeeAccess(cls.EMPLOYEE_ID)
        cls.employee_access.update_step_access(cls.CLIENT_ID, cls.STEP_ACCESSED)

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (co:Company), (e:Employee), (cl:Client), (p:Project), (o:Onboard) , (g:GenericProcess), (s:GenericStep)"
            "detach delete co, e, cl, p, o, g, s"
        ))

    def test_employee_access(self, db):

        cursor = db.graph.run((
            "match (e:Employee)-[:WORKED_ON]->(p)-[:FOR_CLIENT]->(c) "
            "match (p)-[:ACCESSED_STEP]->(s) "
            "where c.company_id='%s' "
            "return s" % self.CLIENT_ID
        ))

        assert cursor.forward() == 1
        assert cursor.current()['s']['step_number'] == self.STEP_ACCESSED
        assert cursor.forward() == 0


class TestApplicationNode(object):

    @classmethod
    def setup_class(cls):

        cls.APP_1 = 'test-crm-app'
        cls.APP_2 = 'test-erp-app'
        cls.APP_3 = 'test-compliance-app'

        cls.app_1 = Application.push_crm(cls.APP_1)
        cls.app_2 = Application.push_erp(cls.APP_2)
        cls.app_3 = Application.push_compliance(cls.APP_3)

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (a:Application)"
            "delete a"
        ))

    def test_application_nodes(self, db):

        cursor = db.graph.run((
            "match (a:Application) "
            "return a"
        ))

        assert cursor.forward() == 1
        assert cursor.current()['a']['name'] == self.APP_1
        assert cursor.forward() == 1
        assert cursor.current()['a']['name'] == self.APP_2
        assert cursor.forward() == 1
        assert cursor.current()['a']['name'] == self.APP_3
        assert cursor.forward() == 0

    def test_crm_label(self, db):

        cursor = db.graph.run((
            "match (a:Crm) "
            "return a"
        ))

        assert cursor.forward() == 1
        assert cursor.current()['a']['name'] == self.APP_1
        assert cursor.forward() == 0

    def test_cloud_label(self, db):

        cursor = db.graph.run((
            "match (a:Cloud) "
            "return a"
        ))

        assert cursor.forward() == 1
        assert cursor.current()['a']['name'] == self.APP_1
        assert cursor.forward() == 0

    def test_erp_label(self, db):

        cursor = db.graph.run((
            "match (a:Erp) "
            "return a"
        ))

        assert cursor.forward() == 1
        assert cursor.current()['a']['name'] == self.APP_2
        assert cursor.forward() == 0

    def test_compliance_label(self, db):

        cursor = db.graph.run((
            "match (a:Compliance) "
            "return a"
        ))

        assert cursor.forward() == 1
        assert cursor.current()['a']['name'] == self.APP_3
        assert cursor.forward() == 0


class TestDatabaseNode(object):

    @classmethod
    def setup_class(cls):

        cls.TYPE = 'sql'
        cls.database = Database.push(cls.TYPE)

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (d:Database)"
            "delete d"
        ))

    def test_database_node(self, db):

        cursor = db.graph.run((
            "match (d:Database) "
            "return d"
        ))

        assert cursor.forward() == 1
        assert cursor.current()['d']['type'] == self.TYPE
        assert cursor.forward() == 0


class TestCrmDatabase(object):

    @classmethod
    def setup_class(cls):

        cls.APP_NAME = 'some crm app'
        cls.TYPE = 'sql'
        cls.crm_db = CrmDatabase(cls.APP_NAME, cls.TYPE)
        cls.crm_db.build()

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (a:Application), (d:Database) "
            "detach delete a, d"
        ))

    def test_crm_database_structure(self, db):

        cursor = db.graph.run((
            "match (:Application)-[:USES_DATABASE]->(db) "
            "return db"
        ))

        assert cursor.forward() == 1
        assert isinstance(cursor.current()['db'], Node)
        assert cursor.current()['db']['type'] == self.TYPE
        assert cursor.forward() == 0


class TestErpDatabase(object):

    @classmethod
    def setup_class(cls):

        cls.APP_NAME = 'some erp app'
        cls.TYPE = 'graphdb'
        cls.erp_db = ErpDatabase(cls.APP_NAME, cls.TYPE)
        cls.erp_db.build()

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (a:Application), (d:Database) "
            "detach delete a, d"
        ))

    def test_erp_database_structure(self, db):

        cursor = db.graph.run((
            "match (:Application)-[:USES_DATABASE]->(db) "
            "return db"
        ))

        assert cursor.forward() == 1
        assert isinstance(cursor.current()['db'], Node)
        assert cursor.current()['db']['type'] == self.TYPE
        assert cursor.forward() == 0


class TestComplianceDatabase(object):

    @classmethod
    def setup_class(cls):

        cls.APP_NAME = 'some compliance app'
        cls.TYPE = 'mysql'
        cls.compliance_db = ComplianceDatabase(cls.APP_NAME, cls.TYPE)
        cls.compliance_db.build()

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (a:Application), (d:Database) "
            "detach delete a, d"
        ))

    def test_compliance_database_structure(self, db):

        cursor = db.graph.run((
            "match (:Application)-[:USES_DATABASE]->(db) "
            "return db"
        ))

        assert cursor.forward() == 1
        assert isinstance(cursor.current()['db'], Node)
        assert cursor.current()['db']['type'] == self.TYPE
        assert cursor.forward() == 0


class TestEmployeeAppAccess(object):

    @classmethod
    def setup_class(cls):

        cls.ID = '1122452335'
        cls.EMAIL = 'gpwn@fuzz.org'
        cls.employee = Employee()
        cls.employee.push(cls.ID, cls.EMAIL)

        cls.APP_1 = 'test-crm-app'
        cls.app_1 = Application.push_crm(cls.APP_1)

        cls.employee_app_access = EmployeeAppAccess('Crm', cls.ID)
        cls.employee_app_access.build()

    @classmethod
    def teardown_class(cls):
        _db.graph.run((
            "match (n) "
            "detach delete n"
        ))

    def test_compliance_database_structure(self, db):

        cursor = db.graph.run((
            "match (:Employee)-[:HAS_ACCESS_TO]->(app) "
            "return app"
        ))

        assert cursor.forward() == 1
        assert isinstance(cursor.current()['app'], Node)
        assert cursor.forward() == 0