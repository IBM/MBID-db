import psycopg2
import pdb
from sqlalchemy import create_engine
import sqlalchemy.orm as orm
import os
import sys
import pandas as pd
import numpy as np
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from config.globals import ENVIRONMENT
from load_db.UKBIOBANK import get_file_set


from botocore.client import Config
import ibm_boto3
import types
import os




if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        cos_client = ibm_boto3.client('s3',
                                      ibm_api_key_id=app.config['COS_CREDENTIALS_UKBB']['apikey'],
                                      ibm_service_instance_id=app.config['COS_CREDENTIALS_UKBB'][
                                          'resource_instance_id'],
                                      ibm_auth_endpoint=app.config['AUTH_ENDPOINT_UKBB'],
                                      config=Config(signature_version='oauth'),
                                      endpoint_url=app.config['SERVICE_ENDPOINT_UKBB'])
        bucket_name = app.config['BUCKET_UKBB']
        missing_subjects = {}
        for modality in [20227,20250, 20252]:
            missing_subjects[modality] = dict()
            existing_file_set = get_file_set(cos_client, bucket_name,str(modality))
            existing_file_set_subjects = [int(s.split('_')[0]) for s in existing_file_set]
            postgresql_url = app.config['SQLALCHEMY_DATABASE_UKBB_URI']
            engine = create_engine(postgresql_url)
            SessionSiteDB = orm.sessionmaker(bind=engine)
            sessionSiteDB = SessionSiteDB()
            subjects_db = sessionSiteDB.execute("""SELECT *
                FROM structured_data.ukbb_data
                where field_id={0};""".format(modality))
            subjects_db_list = [r[0] for r in subjects_db]
            # yields the elements in `existing_file_set_subjects` that are NOT in `subjects_db_list`
            storage_not_db = np.setdiff1d(existing_file_set_subjects, subjects_db_list)
            missing_subjects[modality]['storage_not_db'] = storage_not_db.tolist()
            print('Number of objects in storage not present in db for modality {1}: {0}'.format(str(len(storage_not_db)),modality))
            # yields the elements in `subjects_db_list` that are NOT in `existing_file_set_subjects`
            db_not_storage = np.setdiff1d(subjects_db_list, existing_file_set_subjects)
            missing_subjects[modality]['db_not_storage'] = db_not_storage.tolist()
            print('Number of objects in db not present in storage modality {1}: {0}'.format(str(len(db_not_storage)),modality))
            interesection_list = list(set(existing_file_set_subjects) & set(subjects_db_list))
            print('Number of objects present in db and in storage for  modality {1}: {0}'.format(str(len(interesection_list)),
                                                                                            modality))
        missing_subjects_df = pd.DataFrame.from_dict(missing_subjects,orient='index').transpose()
        missing_subjects_df.to_csv('missing_subjects.csv')
