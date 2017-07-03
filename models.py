import arrow

from extensions import db


class Client(db.Model):
    '''define the client node'''
    __primarykey__ = 'company_id'

    person = db.Label()
    
    company_id = db.Property()
    company_name = db.Property()

    has_onboard = db.RelatedTo('Onboard')

    @staticmethod
    def create(company_id, company_name):
        client = Client()
        client.person = True
        client.company_id = company_id
        client.company_name = company_name
        db.graph.create(client)
        return client

    @staticmethod
    def list_all():
        '''list all clients'''
        return [_ for _ in Client.select(db.graph)]

    @staticmethod
    def list_all_with_compliance_status():
        '''get a list of all clients with compliance status'''
        cursor = db.graph.run((
            "match (c:Client)-[:HAS_ONBOARD]->(o) "
            "return c, o.completed AS completed, o.valid_onboard AS v "
            "order by c.company_name"
        ))
        return [{
            'client': result['c'],
            'completed': result['completed'], 
            'valid_onboard': result['v']} for result in cursor]

    @staticmethod
    def list_all_with_document_status():
        '''get a list of all clients with document status'''
        cursor = db.graph.run((
            "match (c:Client)-[:HAS_ONBOARD]->()-[:MISSING_DOCUMENT]->(d)-[:FOR_STEP]->(s) "
            "return c, d, s "
            "order by c.company_name, s.step_number"
        ))
        return [{
            'client': result['c'],
            'document_type': result['d']['document_type'],
            'step_number': result['s']['step_number']} for result in cursor]


class Onboard(db.Model):
    '''define the onboard node'''
    completed = db.Property()
    valid_onboard = db.Property()
    time_created = db.Property()
    time_completed = db.Property()

    has_completed = db.RelatedTo('GenericStep')
    invalid = db.RelatedTo('GenericStep')
    must_follow = db.RelatedTo('GenericProcess')
    missing_document = db.RelatedTo('GenericDocument')
    submitted_document = db.RelatedTo('GenericDocument')
    has_activity = db.RelatedTo('Activity')

    @staticmethod
    def create():
        onboard = Onboard()
        
        onboard.completed = False
        onboard.valid_onboard = True
        
        a = arrow.utcnow()
        onboard.time_created = a.timestamp
        onboard.time_completed = None

        db.graph.create(onboard)
        return onboard

    @staticmethod
    def compute_average():
        '''calculate the average time to completion'''
        ttc = [_.time_completed - _.time_created for _ in Onboard.select(db.graph) if _.time_completed]
        if ttc:
            ave_ttc = int(round(float(sum(ttc)) / len(ttc)))
            return ave_ttc
        return None


class BuildClientOnboard(object):
    '''build the structure/relationships around the client node'''
    def __init__(self, company_id, company_name):
        self.client = Client.create(company_id, company_name)
        self.onboard = Onboard.create()

    def init_rels(self):
        self.client.has_onboard.add(self.onboard)
        db.graph.push(self.client)
        return 'initial client steps structure built'

    def init(self):
        self.init_rels()
        return 'initial client structure built'


class GenericProcess(db.Model):

    has_step = db.RelatedTo('GenericStep')
    first_step = db.RelatedTo('GenericStep')
    last_step = db.RelatedTo('GenericStep')
    next = db.RelatedTo('GenericStep')
    requires_document = db.RelatedTo('GenericDocument')

    @staticmethod
    def create():
        generic = GenericProcess()
        db.graph.create(generic)
        return generic

    @staticmethod
    def get_steps():
        return db.graph.run((
          "MATCH (:GenericProcess)-[:NEXT*]->(s) "
          "RETURN s ORDER BY s.step_number"
        ))


class GenericStep(db.Model):
    
    task_name = db.Property()
    step_number = db.Property()
    duration = db.Property()

    next = db.RelatedTo('GenericStep')
    depends_on = db.RelatedTo('GenericStep')
    needs_document = db.RelatedTo('GenericDocument')

    @staticmethod
    def create(task_name, step_number, step_duration):
        step = GenericStep()
        step.task_name = task_name
        step.step_number = step_number
        step.duration = step_duration
        db.graph.create(step)
        return step

    @staticmethod
    def all():
        return [_ for _ in GenericStep.select(db.graph)]

    @staticmethod
    def get_by_step_number(step_number):
        return GenericStep.select(db.graph).where(step_number=step_number).first()


class GenericDocument(db.Model):

    document_id = db.Property() 
    document_type = db.Property()

    for_step = db.RelatedTo('GenericStep')

    @staticmethod
    def create(document_id, document_type):
        document = GenericDocument()
        document.document_id = document_id
        document.document_type = document_type
        db.graph.create(document)
        return document


class BuildGenericProcess(object):

    def __init__(self):
        self.generic = GenericProcess.create()
        self.steps = []
        self.documents = []
        self.tasks = [{
            'task_name': 'get signed contracts', 
            'duration': 3
        }, {
            'task_name': 'get compliance documents', 
            'duration': 4
        }, {
            'task_name': 'compliance review', 
            'duration': 3,
        }, {
            'task_name': 'countersign contracts', 
            'duration': 5,
            'depends_on': [0, 1, 2]
        }, {
            'task_name': 'account activation', 
            'duration': 3,
            'depends_on': [3]
        }]
        self.document_metadata = [{            
            'type': 'signed contract', 
            'for_step': 0
        }, {
            'type': 'personal identification', 
            'for_step': 1
        }, {
            'type': 'tax identification', 
            'for_step': 1
        }, {
            'type': 'articles of incorporation', 
            'for_step': 1
        }, {
            'type': 'professional license', 
            'for_step': 1
        }, {
            'type': 'miscellaneous', 
            'for_step': 1
        }, {
            'type': 'compliance review', 
            'for_step': 2
        }, {
            'type': 'countersign contracts', 
            'for_step': 3
        }, {
            'type': 'account activation', 
            'for_step': 4
        }]
        for i in range(len(self.document_metadata)):
            self.document_metadata[i]['id'] = i

    def init_steps(self):
        for step_number, task in enumerate(self.tasks):
            self.steps.append(GenericStep.create(
                task['task_name'], step_number, task['duration'])
            )
        return self.steps

    def init_steps_rels(self):
        
        prior_step = None
        i = 0
        
        for each_step, task in zip(self.steps, self.tasks):
            
            if prior_step:
        
                prior_step.next.add(each_step)
                db.graph.push(prior_step)

            if i == 0:
                self.generic.first_step.add(each_step)
                self.generic.next.add(each_step)
            
            if i == len(self.steps)-1:
                self.generic.last_step.add(each_step)
            
            self.generic.has_step.add(each_step)
            
            i += 1

            if task.get('depends_on') is not None:
                for each_depend in task['depends_on']:
                    each_step.depends_on.add(self.steps[each_depend])
                    db.graph.push(each_step)
            
            prior_step = each_step

        db.graph.push(self.generic)
        return 'generic process steps structure built'

    def init_docs(self):
        for document in self.document_metadata:
            self.documents.append(GenericDocument.create(document['id'], document['type']))
        return self.documents

    def init_docs_rels(self):
        for document in self.documents:
            self.generic.requires_document.add(document)
        db.graph.push(self.generic)
        return 'generic process document structure built'

    def init_docs_steps_rels(self):
        for document_number, meta in enumerate(self.document_metadata):
            self.documents[document_number].for_step.add(self.steps[meta['for_step']])
            db.graph.push(self.documents[document_number])
        return 'generic process document step structure built'

    def init(self):
        self.init_steps()
        self.init_steps_rels()
        self.init_docs()
        self.init_docs_rels()
        self.init_docs_steps_rels()
        return 'generic process structure built'


class BuildOnboardGenericProcess(object):

    def __init__(self, company_id):
        self.onboard = list(Client.select(db.graph).where(
            company_id=company_id
        ).first().has_onboard)[0]
        self.generic = GenericProcess.select(db.graph).first()

    def init_rels(self):
        self.onboard.must_follow.add(self.generic)

        for document in GenericDocument.select(db.graph):
            self.onboard.missing_document.add(document)

        db.graph.push(self.onboard)

        return "onboarding rels added"

    def init(self):
        self.init_rels()
        return 'onboarding structure added to client'


class Activity(db.Model):

    action_taken = db.RelatedTo('Action')
    first_action = db.RelatedTo('Action')
    last_action = db.RelatedTo('Action')

    @staticmethod
    def create():
        activity = Activity()
        db.graph.create(activity)
        return activity


class Action(db.Model):

    number = db.Property()
    taken_at = db.Property()

    has_completed = db.RelatedTo('GenericStep')
    action_taken = db.RelatedTo('Action') 

    @staticmethod
    def create(company_id):
        action = Action()
        a = arrow.utcnow()
        action.number = action.get_num_actions(company_id)
        action.taken_at = a.timestamp
        db.graph.create(action)
        return action

    def _is_client_onboard_structure_built(self, company_id):
        cursor = db.graph.run((
            "match (:Client {company_id: '%s'})-[r:HAS_ONBOARD]->()"
            "return r" % company_id
        ))
        return cursor.forward()

    def _is_onboard_activity_structure_built(self, company_id):
        cursor = db.graph.run((
            "match (:Client {company_id: '%s'})-[:HAS_ONBOARD]->()-[r:HAS_ACTIVITY]->()"
            "return r" % company_id
        ))
        return cursor.forward()

    def _structure_is_built(self, company_id):
        if self._is_client_onboard_structure_built(company_id) and self._is_onboard_activity_structure_built(company_id):
            return True
        return False

    def get_num_actions(self, company_id):
        if self._structure_is_built(company_id):
            cursor = db.graph.run((
                "match (:Client {company_id: '%s'})-[:HAS_ONBOARD]->()-[:HAS_ACTIVITY]->()-[:ACTION_TAKEN*]->(a) "
                "return count(a) as num_actions" % company_id
            ))
            return cursor.next()['num_actions']
        return None

    def add_has_completed_rel(self, company_id, step_number):
        if self._structure_is_built(company_id):
            step = GenericStep.get_by_step_number(step_number)
            self.has_completed.add(step)
            db.graph.push(self)
            return self
        raise LookupError('required graph structure missing')



class BuildOnboardActivity(object):

    def __init__(self, company_id):
        self.onboard = list(Client.select(db.graph).where(
            company_id=company_id
        ).first().has_onboard)[0]
        self.activity = Activity.create()

    def init_activity_rels(self):
        self.onboard.has_activity.add(self.activity)
        db.graph.push(self.onboard)
        return 'built onboard has activity structure'

    def init(self):
        self.init_activity_rels()
        return 'built onboard activity structure'


class BuildAction(object):

    def __init__(self, company_id):
        self.company_id = company_id
        self.onboard = list(Client.select(db.graph).where(
            company_id=company_id
        ).first().has_onboard)[0]
        self.activity = [_ for _ in self.onboard.has_activity][0]
        self.actions = None

    def _num_dependencies(self, step_number):
        cursor = db.graph.run((
            "match (s:GenericStep)-[:DEPENDS_ON*]->(ds) "
            "where s.step_number=%d "
            "return count(ds) AS num_depends" % step_number
        ))
        return cursor.next()['num_depends']

    def _completed_dependencies(self, step_number):
        cursor = db.graph.run((
            "match (s:GenericStep {step_number: %d})-[:DEPENDS_ON*]->(ds) "
            "match (ds)<-[:HAS_COMPLETED]-(action) "
            "match (:Client {company_id: '%s'})-[:HAS_ONBOARD]->()-[:HAS_ACTIVITY]->(activity) "
            "match (activity)-[:ACTION_TAKEN*]->(action) "
            "return distinct ds order by ds.step_number" % (step_number, self.company_id)
        ))
        return [result['ds']['step_number'] for result in cursor]

    def _depends_satisfied(self, step_number):
        number_of_depends = self._num_dependencies(step_number)
        completed_depends = self._completed_dependencies(step_number)
        if number_of_depends == len(completed_depends):
            return True
        return False

    def _mark_onboard_complete(self):
        a = arrow.utcnow()
        self.onboard.completed = True
        self.onboard.time_completed = a.timestamp
        db.graph.push(self.onboard)
        return 'onboard process marked complete'

    def _step_aware_mark_onboard_complete(self):
        '''will mark the onboard process as complete if all the generic steps have been completed'''
        if len(list(self.onboard.has_completed)) == len(GenericStep.all()):
            self._mark_onboard_complete()
        return 'onboard process not complete'

    def _mark_step_complete(self, step_number):
        step = GenericStep.select(db.graph).where(
            step_number=step_number
        ).first()
        self.onboard.has_completed.add(step)
        db.graph.push(self.onboard)
        return "marked step %d as complete" % step_number

    def _mark_step_invalid(self, step_number):
        step = GenericStep.select(db.graph).where(
            step_number=step_number
        ).first()
        self.onboard.invalid.add(step)
        self.onboard.valid_onboard = False
        db.graph.push(self.onboard)
        return "marked step %d as invalid" % step_number

    def _dependency_aware_mark_step_complete(self, step_number):
        if self._depends_satisfied(step_number):
            self._mark_step_complete(step_number)
            return "step marked as valid and complete"
        self._mark_step_complete(step_number)
        self._mark_step_invalid(step_number)
        return "step marked as invalid and complete"

    def aware_mark_step_complete(self, step_number):
        self._dependency_aware_mark_step_complete(step_number)
        self._step_aware_mark_onboard_complete()
        return "recorded action for step %d and appropriately adjusted onboard activity" % step_number

    def _update_actions(self):
        cursor = db.graph.run((
            "match (:Activity)-[:ACTION_TAKEN*]->(action) "
            "return action"
        ))
        self.actions = [_ for _ in cursor]
        return self.actions

    def _is_first_action(self):
        cursor = db.graph.run((
            "match (:Client {company_id: '%s'})-[:HAS_ONBOARD]->()-[:HAS_ACTIVITY]->()-[:ACTION_TAKEN]->(action) "
            "return action" % self.company_id
        ))
        return not cursor.forward()

    def _add_first_action(self):
        action = Action.create(self.company_id)
        db.graph.push(action)
        self.activity.action_taken.add(action)
        self.activity.first_action.add(action)
        self.activity.last_action.add(action)
        db.graph.push(self.activity)
        return action

    def _get_and_move_last_action(self, new_action):

        last_action = [_ for _ in self.activity.last_action][0]

        last_action.action_taken.add(new_action)
        self.activity.last_action.remove(last_action)
        self.activity.last_action.add(new_action)

        db.graph.push(self.activity)
        db.graph.push(last_action)

        return last_action

    def _add_next_action(self):
        new_action = Action.create(self.company_id)
        db.graph.push(new_action)
        last_action = self._get_and_move_last_action(new_action)
        db.graph.push(last_action)
        return new_action

    def _new_action(self):
        if self._is_first_action():
            return self._add_first_action()
        return self._add_next_action()

    def new_action(self, step_number):
        '''add a new action node optionally marking a step as completed'''
        action = self._new_action()
        action.add_has_completed_rel(self.company_id, step_number)
        # self.aware_mark_step_complete(step_number)
        return action 


class UpdateClientOnboard(object):
    '''logic for updating the onboard node and structure in vicinity'''

    def __init__(self, company_id):
        self.company_id = company_id
        self.onboard = list(Client.select(db.graph).where(
            company_id=company_id
        ).first().has_onboard)[0]

    def submit_document(self, document_id):
        document = GenericDocument.select(db.graph).where(document_id=document_id).first()
        self.onboard.submitted_document.add(document)
        self.onboard.missing_document.remove(document)
        db.graph.push(self.onboard)
        return 'marked document_%d as submitted' % document_id


class Company(db.Model):

    __primarykey__ = 'name'

    name = db.Property()

    @staticmethod
    def push(company_name):
        company = Company()
        company.name = company_name
        db.graph.push(company)
        return company


class Employee(db.Model):

    __primarykey__ = 'id'

    person = db.Label()
    id = db.Property()
    email = db.Property()
    first_name = db.Property()
    last_name = db.Property()
    street_address = db.Property()
    city = db.Property()
    state = db.Property()
    zip_code = db.Property()

    works_for = db.RelatedTo('Company')
    worked_on = db.RelatedTo('Project')
    has_access_to = db.RelatedTo('Application')

    @staticmethod
    def push(employee_id, employee_email):
        employee = Employee()
        employee.person = True
        employee.id = employee_id
        employee.email = employee_email
        db.graph.push(employee)
        return employee


class BuildEmployeeCompany(object):
    
    def __init__(self, employee_id, employee_email, company_name):
        self.employee = Employee.push(employee_id, employee_email)
        self.company = Company.push(company_name)

    def init_rels(self):
        self.employee.works_for.add(self.company)
        db.graph.push(self.employee)
        return 'initial employee structure built'

    def init(self):
        self.init_rels()
        return 'built employee company structure'


class Project(db.Model):

    for_onboard = db.RelatedTo('Onboard')
    for_client = db.RelatedTo('Client')
    accessed_step = db.RelatedTo('GenericStep')

    @staticmethod
    def create():
        project = Project()
        db.graph.create(project)
        return project


class BuildEmployeeInvolvement(object):

    def __init__(self, employee_id, client_id):
        self.employee = Employee.select(db.graph).where(
          "_.id='%s'" % employee_id
        ).first()
        self.project = Project.create()
        self.client = Client.select(db.graph).where(
          "_.company_id='%s'" % client_id
        ).first()
        self.onboard = list(self.client.has_onboard)[0]

    def init_rels(self):
        self.employee.worked_on.add(self.project)
        self.project.for_onboard.add(self.onboard)
        self.project.for_client.add(self.client)
        db.graph.push(self.employee)
        db.graph.push(self.project)
        return 'added employee involvement'

    def init(self):
        self.init_rels()
        return 'built employee involvement structure'


class UpdateEmployeeAccess(object):

    def __init__(self, employee_id):
        self.employee_id = employee_id

    def update_step_access(self, client_id, step_number):
        return db.graph.run(
            "MATCH (e:Employee)-[:WORKED_ON]->(p:Project)-[:FOR_CLIENT]->(c:Client) " +
            "WHERE e.id='%s' AND c.company_id='%s' " % (self.employee_id, client_id) +
            "MATCH (c)-[:HAS_ONBOARD]->()-[:MUST_FOLLOW]->()-[:HAS_STEP]->(s) " + 
            "WHERE s.step_number=%s " % str(step_number) +
            "MERGE (p)-[:ACCESSED_STEP]->(s) " +
            "RETURN e"
        )


class Application(db.Model):

    __primarykey__ = 'name'

    crm = db.Label()
    erp = db.Label()
    compliance = db.Label()
    cloud = db.Label()

    name = db.Property()

    # accessed_by = db.RelatedFrom('Employee')
    uses_database = db.RelatedTo('Database')

    @staticmethod
    def push_crm(app_name):
        crm_app = Application()
        crm_app.crm = True
        crm_app.cloud = True
        crm_app.name = app_name
        db.graph.push(crm_app)
        return crm_app

    @staticmethod
    def push_erp(app_name):
        erp_app = Application()
        erp_app.erp = True
        erp_app.name = app_name
        db.graph.push(erp_app)
        return erp_app

    @staticmethod
    def push_compliance(app_name):
        comp_app = Application()
        comp_app.compliance = True
        comp_app.name = app_name
        db.graph.push(comp_app)
        return comp_app
        

class Database(db.Model):
    
    type = db.Property()
    
    in_use_by = db.RelatedFrom('Application')

    @staticmethod
    def push(database_type):
        database = Database()
        database.type = database_type
        db.graph.push(database)
        return database


class BuildCrmDatabase(object):
   
    def __init__(self, app_name, database_type):
        self.crm_app = Application.push_crm(app_name)
        self.database = Database.push(database_type)

    def build(self):
        self.crm_app.uses_database.add(self.database)
        db.graph.push(self.crm_app)
        db.graph.push(self.database)
        return 'structure built'


class BuildErpDatabase(object):
   
    def __init__(self, app_name, database_type):
        self.erp_app = Application.push_erp(app_name)
        self.database = Database.push(database_type)

    def build(self):
        self.erp_app.uses_database.add(self.database)
        db.graph.push(self.erp_app)
        db.graph.push(self.database)
        return 'structure built'


class BuildComplianceDatabase(object):
   
    def __init__(self, app_name, database_type):
        self.comp_app = Application.push_compliance(app_name)
        self.database = Database.push(database_type)

    def build(self):
        self.comp_app.uses_database.add(self.database)
        db.graph.push(self.comp_app)
        db.graph.push(self.database)
        return 'structure built'
   

class EmployeeAppAccess(object):

    def __init__(self, app_label, employee_id):
        '''app label means node label for application node'''
        self.app_label = app_label
        self.employee_id = employee_id

    def build(self):
        employee = Employee.select(db.graph).where(
          "_.id='%s'" % self.employee_id
        ).first()
        
        app = Application.select(db.graph).where(
          "'%s' IN labels(_)" % self.app_label
        ).first()

        employee.has_access_to.add(app)
        db.graph.push(employee)
        return 'built employee app access'


def build_model():
    '''builds a sample data set using the model'''

    COMPANY_ID_1 = 'company_id_1'
    COMPANY_ID_2 = 'company_id_2'

    # build the generic onboard process in the database
    generic = BuildGenericProcess()
    generic.init()

    # initialize a new client by creating client and onboard structure
    client_1 = BuildClientOnboard(COMPANY_ID_1, 'company_name_1')
    client_1.init()
    
    client_2 = BuildClientOnboard(COMPANY_ID_2, 'company_name_2')
    client_2.init()

    # initialize the structures for a clients onboard and the generic process
    cli_1_onboard = BuildOnboardGenericProcess(COMPANY_ID_1)
    cli_1_onboard.init()
    
    cli_2_onboard = BuildOnboardGenericProcess(COMPANY_ID_2)
    cli_2_onboard.init()

    # initialize some employees
    employee_1 = BuildEmployeeCompany('employee_id_1', 'employee_email_1', 'Citi')
    employee_1.init()

    employee_2 = BuildEmployeeCompany('employee_id_2', 'employee_email_2', 'Citi')
    employee_2.init()

    # mark employees as involved in work with particular clients
    empl_cust_involve_1 = BuildEmployeeInvolvement('employee_id_1', COMPANY_ID_1)
    empl_cust_involve_1.init()

    empl_cust_involve_2 = BuildEmployeeInvolvement('employee_id_2', COMPANY_ID_2)
    empl_cust_involve_2.init()

    # track which steps have been accessed by a given employee
    customer_access_1 = UpdateEmployeeAccess('employee_id_1')
    customer_access_1.update_step_access(COMPANY_ID_1, 4)

    customer_access_2 = UpdateEmployeeAccess('employee_id_2')
    customer_access_2.update_step_access(COMPANY_ID_2, 2)

    # create some databases for the company
    crm = BuildCrmDatabase('Salesforce', 'cloud')
    crm.build()

    erp = BuildErpDatabase('SAP', 'Oracle1')
    erp.build()

    compliance = BuildComplianceDatabase('Actimize', 'SqlServer1')
    compliance.build()

    # build some structure to show which employees access which databases
    app_access_1 = EmployeeAppAccess('Crm', 'employee_id_1')
    app_access_1.build()
    
    app_access_2 = EmployeeAppAccess('Erp', 'employee_id_2')
    app_access_2.build()
    
    app_access_3 = EmployeeAppAccess('Compliance', 'employee_id_2')
    app_access_3.build()

    update_cli_1 = UpdateClientOnboard(COMPANY_ID_1)
    for i in range(len(GenericStep.all())):
        update_cli_1.aware_mark_step_complete(i)

    update_cli_2 = UpdateClientOnboard(COMPANY_ID_2)
    update_cli_2.aware_mark_step_complete(3)
    
    return 'model built'

def build_clients():

    COMPANY_ID_0 = 'another-test-comp-id'
    COMPANY_NAME_0 = 'another-test-comp-name'

    COMPANY_ID_1 = 'one-more-test-comp-id'
    COMPANY_NAME_1 = 'one-more-test-comp-name'

    new_client_0 = BuildClientOnboard(COMPANY_ID_0, COMPANY_NAME_0)
    new_client_0.init_rels()
    new_client_1 = BuildClientOnboard(COMPANY_ID_1, COMPANY_NAME_1)
    new_client_1.init_rels()

    return 'clients created'