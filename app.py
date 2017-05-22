from flask import Flask
from flask_py2neo import Py2Neo

app = Flask(__name__)
app.config.update({
  'PY2NEO_HOST': 'neo4j'    
})

db = Py2Neo(app)


class Client(db.Model):

    __primarykey__ = 'id'

    person = db.Label()
    id = db.Property()
    email = db.Property()
    has_onboard = db.RelatedTo('Onboard')

    @staticmethod
    def create(client_id, client_email):
        client = Client()
        client.person = True
        client.email = client_email
        client.id = client_id
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
    completed = db.Property()

    @staticmethod
    def create(task_name, step_number):
        step = Step()
        step.task_name = task_name
        step.step_number = step_number
        step.completed = False
        db.graph.create(step)
        return step


class BuildClient(object):

    def __init__(self, client_id, client_email):
        self.client = Client.create(client_id, client_email)
        self.onboard = Onboard.create()
        self.steps = []
        self.tasks = [
            {'task_name': 'get signed contracts'}, 
            {'task_name': 'get compliance documents'},
            {'task_name': 'compliance review'},
            {'task_name': 'countersign contracts'},
            {'task_name': 'account activation'},
        ]

    def init_steps(self):
        for step_number, task in enumerate(self.tasks):
            self.steps.append(Step.create(task['task_name'], step_number))
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

    works_for = db.RelatedTo('Company')
    worked_on = db.RelatedTo('Project')

    @staticmethod
    def create(employee_id, employee_email, first_name, last_name):
        employee = Employee()
        employee.person = True
        employee.id = employee_id
        employee.email = employee_email
        employee.first_name = first_name
        employee.last_name = last_name
        db.graph.create(employee)
        return employee

class BuildEmployee(object):
    
    def __init__(self, employee_id, employee_email, first_name, last_name, company_name):
        self.employee = Employee.create(employee_id, employee_email, first_name, last_name)
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
          "_.id='%s'" % client_id
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
            "WHERE e.id='%s' AND c.id='%s' " % (self.employee_id, client_id) +
            "MATCH (c)-[:HAS_ONBOARD]->()-[:HAS_STEP]->(s) " + 
            "WHERE s.step_number=%s " % str(step_number) +
            "MERGE (p)-[:ACCESSED_STEP]->(s) " +
            "RETURN e"
        )


@app.route('/create/<string:name>')
def create(name):
    db.graph.run("CREATE (n:User {name: '%s'})" % name) # noqa
    return jsonify('user %s created' % name)
