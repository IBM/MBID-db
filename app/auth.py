from flask import jsonify, g, current_app
from flask_httpauth import HTTPTokenAuth
from .models import AuthKeys


auth = HTTPTokenAuth('siteGUID')



@auth.verify_token
def verify_token(auth_guid):
    g.auth_key = AuthKeys.query.filter_by(guid=auth_guid).first()
    if g.auth_key is None:
        return False
    return True

@auth.error_handler
def unauthorized():
    response = jsonify({'status': 401, 'error': 'unauthorized',
                        'message': 'please authenticate'})
    response.status_code = 401
    return response
