import psycopg2
import pdb
from sqlalchemy import create_engine
import sqlalchemy.orm as orm
import os
import sys
import zipfile
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
from io import BytesIO
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
        unzipped_folder = 'unzipped/'
        unzipped_file_set = list(get_file_set(cos_client, bucket_name, 'unzipped/'))
        unzipped_file_set = [x.split('unzipped/')[1].split('/')[0] for x in unzipped_file_set]
        missing_subjects = {}
        # Only start with t1
        #for modality in [20227,20250, 20252]:
        for modality in [20252]:
            missing_subjects[modality] = dict()
            existing_file_set = list(get_file_set(cos_client, bucket_name,str(modality)))
            for existing_zip_file in tqdm(existing_file_set):
                if existing_zip_file.endswith('.zip') and existing_zip_file not in unzipped_file_set:
                    zip_obj = cos_client.get_object(Bucket=bucket_name, Key=existing_zip_file)

                    buffer = BytesIO(zip_obj["Body"].read())

                    z = zipfile.ZipFile(buffer, 'r')

                    for filename in z.namelist():
                        if (modality == 20227 and (filename.endswith('rfMRI.json') or filename.endswith('rfMRI.nii.gz')))\
                                or (modality == 20250 and (filename.endswith('AP.nii.gz') or filename.endswith('AP.bval')
                        or filename.endswith('AP.bvec') or filename.endswith('AP.json'))) or (modality == 20252 and (filename.endswith('T1_orig_defaced.nii.gz')
                                                                                                                     or filename.endswith('T1.json'))):

                            file_info = z.getinfo(filename)

                            # Now copy the files to the 'unzipped' S3 folder


                            response = cos_client.put_object(

                                Body=z.open(filename).read(),

                                Bucket=bucket_name,

                                Key=unzipped_folder+existing_zip_file+'/'+filename

                            )

