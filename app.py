from flask import Flask
from flask_py2neo import Py2Neo

app = Flask(__name__)
app.config.update({
  'PY2NEO_HOST': 'neo4j'    
})

db = Py2Neo(app)

class Client(db.Model):

    person = db.Label()
    email = db.Property()
    has_onboard = db.RelatedTo('Onboard')

class Onboard(db.Model):

    completed = db.Property()
    has_step = db.RelatedTo('Step')

class Step(db.Model):

    task_name = db.Property()
    step_number = db.Property()
    completed = db.Property()

def create_client(email):
    client = Client()
    client.email = email

    onboard = Onboard()
    onboard.completed = False
    
    client.has_onboard.add(onboard)

    for i in range(5):
        step = Step()
        step.step_number = i
        step.completed = False

        onboard.has_step.add(step)

        db.graph.create(step)

    db.graph.create(client)

    return client

@app.route('/')
def index():
    return 'hi'

@app.route('/create/<string:name>')
def create(name):
    db.graph.run("CREATE (n:User {name: '%s'})" % name) # noqa
    return jsonify('user %s created' % name)

