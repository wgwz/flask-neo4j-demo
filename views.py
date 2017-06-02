from flask import Blueprint, jsonify, render_template
from extensions import db
from models import build_model


bp = Blueprint('bp', __name__)


@bp.route('/')
def index():
    return render_template('index.html')


@bp.route('/build')
def build():
    
    db.graph.run("MATCH (n) DETACH DELETE n")
    
    m = build_model()

    return jsonify({'status': m})
