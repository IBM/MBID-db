import os
from flask import Flask, jsonify, g, request
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS, cross_origin
from flask_migrate import Migrate
from psycopg2.extensions import register_adapter, AsIs
import numpy

db = SQLAlchemy()
migrate = Migrate()

def create_app(config_name):
    """Create an application instance."""
    app = Flask(__name__)

    CORS(app)

    # apply configuration
    cfg = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)),
                       'config',  config_name + '.py')
    app.config.from_pyfile(cfg)

    # initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)
    from .api_v1 import api as api_blueprint
    app.register_blueprint(api_blueprint)

    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)

    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)

    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)
    return app