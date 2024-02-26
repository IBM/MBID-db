import os
from flask import request, got_request_exception

from app import create_app, db
from app.models import *
from config.globals import ENVIRONMENT

app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))


if __name__ == '__main__':
    with app.app_context():
        db.create_all()

    def log_exception(exception):
        """ Log an exception to our logging framework """
        app.logger.error('Got exception during processing: %s, with the path %s'
                         % (exception, request.url))

    got_request_exception.connect(log_exception)

    app.run(threaded=True, host='0.0.0.0')