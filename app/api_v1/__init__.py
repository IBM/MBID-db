from flask import Blueprint
from flask_restful import Api

from app.auth import auth
#from .person_study_resource import PersonStudyResource

api = Blueprint('api_v1', __name__, url_prefix='/api/v1')



@api.before_request
@auth.login_required
def before_request():
    """All routes in this blueprint require authentication."""
    pass


apiResource = Api(catch_all_404s=True)

apiResource.init_app(api)


#apiResource.add_resource(PersonStudyResource, '/person_study', '/person_study/')
