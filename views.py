from flask import Blueprint, jsonify, render_template
from extensions import db
from models import build_model, build_clients
from models import Client, Onboard


bp = Blueprint('bp', __name__)

@bp.route('/')
def index():
    return render_template('index.html')

@bp.route('/compliance')
def compliance():
    return render_template('compliance.html', clients = Client.list_all_with_compliance_status())

@bp.route('/funnel')
def funnel():
    return render_template('funnel.html')

@bp.route('/gap_analysis')
def gap_analysis():
    return render_template('gap_analysis.html', clients = Client.list_all_with_document_status())

@bp.route('/kpi')
def kpi():
    return render_template('kpi.html', average_ttc = Onboard.compute_average())

@bp.route('/client_metric')
def client_metric():
    return render_template('client_metric.html')

@bp.route('/impact_analysis')
def impact_analysis():
    return render_template('impact_analysis.html')

@bp.route('/provenance')
def provenance():
    return render_template('provenance.html')

@bp.route('/build')
def build():
    
    db.graph.run("MATCH (n) DETACH DELETE n")
    
    m = build_model()

    return jsonify({'status': m})

@bp.route('/create_clients')
def create_clients():

    db.graph.run("match (n) detach delete n")

    m = build_clients()

    return jsonify({'status': m})
