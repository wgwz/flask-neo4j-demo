from flask import Flask, jsonify
from flask_py2neo import Py2Neo

app = Flask(__name__)
app.config.update({
  'PY2NEO_HOST': 'neo4j'    
})
db = Py2Neo(app)


class Client(db.Model):

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


class Onboard(db.Model):

    completed = db.Property()
    has_step = db.RelatedTo('Step')

    @staticmethod
    def create():
        onboard = Onboard()
        onboard.completed = False
        db.graph.create(onboard)
        return onboard


class Step(db.Model):

    task_name = db.Property()
    step_number = db.Property()
    duration = db.Property()
    completed = db.Property()

    @staticmethod
    def create(task_name, step_number, step_duration):
        step = Step()
        step.task_name = task_name
        step.step_number = step_number
        step.duration = step_duration
        step.completed = False
        db.graph.create(step)
        return step


class BuildClient(object):

    def __init__(self, company_id, company_name):
        self.client = Client.create(company_id, company_name)
        self.onboard = Onboard.create()
        self.steps = []
        self.tasks = [
            {'task_name': 'get signed contracts', 'duration': 3}, 
            {'task_name': 'get compliance documents', 'duration': 4},
            {'task_name': 'compliance review', 'duration': 3},
            {'task_name': 'countersign contracts', 'duration': 5},
            {'task_name': 'account activation', 'duration': 3},
        ]

    def init_steps(self):
        for step_number, task in enumerate(self.tasks):
            self.steps.append(Step.create(task['task_name'], step_number, task['duration']))
        return self.steps

    def init_steps_rels(self):
        self.client.has_onboard.add(self.onboard)
        for each_step in self.steps:
            self.onboard.has_step.add(each_step)
        db.graph.push(self.client)
        db.graph.push(self.onboard)
        return 'initial client steps structure built'


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
    def create(employee_id, employee_email):
        employee = Employee()
        employee.person = True
        employee.id = employee_id
        employee.email = employee_email
        db.graph.create(employee)
        return employee

class BuildEmployee(object):
    
    def __init__(self, employee_id, employee_email, company_name):
        self.employee = Employee.create(employee_id, employee_email)
        self.company = Company.push(company_name)

    def init_rels(self):
        self.employee.works_for.add(self.company)
        db.graph.push(self.employee)
        return 'initial employee structure built'

class Project(db.Model):

    involved_in = db.RelatedTo('Onboard')
    for_client = db.RelatedTo('Client')
    accessed_step = db.RelatedTo('Step')

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
        self.project.involved_in.add(self.onboard)
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
            "MATCH (c)-[:HAS_ONBOARD]->()-[:HAS_STEP]->(s) " + 
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
    client_1.init_steps()
    client_1.init_steps_rels()
    
    client_2 = BuildClient('company_id_2', 'company_name_2')
    client_2.init_steps()
    client_2.init_steps_rels()
    
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


@app.route('/build')
def build():
    
    db.graph.run("MATCH (n) DETACH DELETE n")
    
    m = build_model()

    return jsonify({'status': m})
