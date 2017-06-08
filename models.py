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
        return [_ for _ in Client.select(db.graph)]

    @staticmethod
    def list_all_with_compliance_status():
        cursor = db.graph.run((
            "match (c:Client)-[:HAS_ONBOARD]->(o) "
            "return c, o.completed AS completed, o.valid_onboard AS v"
        ))
        return [{
            'client': result['c'],
            'completed': result['completed'], 
            'valid_onboard': result['v']} for result in cursor]


class Onboard(db.Model):
    '''define the onboard node'''
    completed = db.Property()
    valid_onboard = db.Property()

    has_completed = db.RelatedTo('GenericStep')
    must_follow = db.RelatedTo('GenericProcess')

    @staticmethod
    def create():
        onboard = Onboard()
        onboard.completed = False
        onboard.valid_onboard = False
        db.graph.create(onboard)
        return onboard


class BuildClient(object):
    '''build the structure/relationships around the client node'''
    def __init__(self, company_id, company_name):
        self.client = Client.create(company_id, company_name)
        self.onboard = Onboard.create()

    def init_rels(self):
        self.client.has_onboard.add(self.onboard)
        db.graph.push(self.client)
        return 'initial client steps structure built'


class GenericProcess(db.Model):

    has_step = db.RelatedTo('GenericStep')
    first_step = db.RelatedTo('GenericStep')
    last_step = db.RelatedTo('GenericStep')
    next = db.RelatedTo('GenericStep')

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

    @staticmethod
    def create(task_name, step_number, step_duration):
        step = GenericStep()
        step.task_name = task_name
        step.step_number = step_number
        step.duration = step_duration
        step.completed = False
        db.graph.create(step)
        return step


class BuildGeneric(object):

    def __init__(self):
        self.generic = GenericProcess.create()
        self.steps = []
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
        return 'generic process structure built'
    

class BuildClientOnboard(object):

    def __init__(self, company_id):
        self.onboard = list(Client.select(db.graph).where(
            company_id=company_id
        ).first().has_onboard)[0]
        self.generic = GenericProcess.select(db.graph).first()

    def init_rels(self):
        self.onboard.must_follow.add(self.generic)
        db.graph.push(self.onboard)
        return "structure built"


class UpdateClientOnboard(object):
    # TODO: finish can step be completed AKA have dependencies be met
    def __init__(self, company_id):
        self.onboard = list(Client.select(db.graph).where(
            company_id=company_id
        ).first().has_onboard)[0]

    def can_step_be_completed(self, step_number):
        step = GenericStep.select(db.graph).where(
            step_number=step_number
        ).first()
        steps_dependencies = list(step.depends_on)
        return steps_dependencies

    def mark_step_complete(self, step_number):
        step = GenericStep.select(db.graph).where(
            step_number=step_number
        ).first()
        self.onboard.has_completed.add(step)
        db.graph.push(self.onboard)
        return "marked step %d as complete" % step_number


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


class BuildEmployee(object):
    
    def __init__(self, employee_id, employee_email, company_name):
        self.employee = Employee.push(employee_id, employee_email)
        self.company = Company.push(company_name)

    def init_rels(self):
        self.employee.works_for.add(self.company)
        db.graph.push(self.employee)
        return 'initial employee structure built'


class Project(db.Model):

    for_onboard = db.RelatedTo('Onboard')
    for_client = db.RelatedTo('Client')
    accessed_step = db.RelatedTo('GenericStep')

    @staticmethod
    def create():
        project = Project()
        db.graph.create(project)
        return project


class EmployeeInvolvement(object):

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


class EmployeeAccess(object):

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


class CrmDatabase(object):
   
    def __init__(self, app_name, database_type):
        self.crm_app = Application.push_crm(app_name)
        self.database = Database.push(database_type)

    def build(self):
        self.crm_app.uses_database.add(self.database)
        db.graph.push(self.crm_app)
        db.graph.push(self.database)
        return 'structure built'


class ErpDatabase(object):
   
    def __init__(self, app_name, database_type):
        self.erp_app = Application.push_erp(app_name)
        self.database = Database.push(database_type)

    def build(self):
        self.erp_app.uses_database.add(self.database)
        db.graph.push(self.erp_app)
        db.graph.push(self.database)
        return 'structure built'


class ComplianceDatabase(object):
   
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
    
    client_1 = BuildClient('company_id_1', 'company_name_1')
    client_1.init_rels()
    
    client_2 = BuildClient('company_id_2', 'company_name_2')
    client_2.init_rels()
   
    generic = BuildGeneric()
    generic.init_steps()
    generic.init_steps_rels()

    cli_1_onboard = BuildClientOnboard('company_id_1')
    cli_1_onboard.init_rels()
    
    cli_2_onboard = BuildClientOnboard('company_id_2')
    cli_2_onboard.init_rels()

    employee_1 = BuildEmployee('employee_id_1', 'employee_email_1', 'Citi')
    employee_2 = BuildEmployee('employee_id_2', 'employee_email_2', 'Citi')
    
    employee_1.init_rels()
    employee_2.init_rels()

    empl_cust_involve_1 = EmployeeInvolvement('employee_id_1', 'company_id_1')
    empl_cust_involve_2 = EmployeeInvolvement('employee_id_2', 'company_id_2')
    
    empl_cust_involve_1.init_rels()
    empl_cust_involve_2.init_rels()

    customer_access_1 = EmployeeAccess('employee_id_1')
    customer_access_2 = EmployeeAccess('employee_id_2')

    customer_access_1.update_step_access('company_id_1', 4)
    customer_access_2.update_step_access('company_id_2', 2)

    crm = CrmDatabase('Salesforce', 'cloud')
    crm.build()

    erp = ErpDatabase('SAP', 'Oracle1')
    erp.build()

    compliance = ComplianceDatabase('Actimize', 'SqlServer1')
    compliance.build()

    app_access_1 = EmployeeAppAccess('Crm', 'employee_id_1')
    app_access_1.build()
    
    app_access_2 = EmployeeAppAccess('Erp', 'employee_id_2')
    app_access_2.build()
    
    app_access_3 = EmployeeAppAccess('Compliance', 'employee_id_2')
    app_access_3.build()

    return 'model built'

def build_clients():

    COMPANY_ID_0 = 'another-test-comp-id'
    COMPANY_NAME_0 = 'another-test-comp-name'

    COMPANY_ID_1 = 'one-more-test-comp-id'
    COMPANY_NAME_1 = 'one-more-test-comp-name'

    new_client_0 = BuildClient(COMPANY_ID_0, COMPANY_NAME_0)
    new_client_0.init_rels()
    new_client_1 = BuildClient(COMPANY_ID_1, COMPANY_NAME_1)
    new_client_1.init_rels()

    return 'clients created'