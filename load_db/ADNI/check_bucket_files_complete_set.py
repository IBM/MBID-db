from botocore.client import Config
import ibm_boto3
# import types
import os
import sys
import pdb
import pandas as pd
import numpy as np
import pickle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app
from config.globals import ENVIRONMENT


if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        cos_client = ibm_boto3.resource('s3',
                                        ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                        ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                        ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                        config=Config(signature_version='oauth'),
                                        endpoint_url=app.config['SERVICE_ENDPOINT'])
        image_list = []
        bucket = cos_client.Bucket(app.config['BUCKET'])
        for i in bucket.objects.all():
            if i.size != 0 and 'ADNI' in i.key and i.key.endswith('nii.gz'):
                try:
                    image_list.append(i.key)
                except:
                    continue
        mri_list_df = pd.read_csv('../../data/ADNI_metadata/MRILIST.csv')
        mp_rage_df = mri_list_df[mri_list_df['SEQUENCE'] == 'MP-RAGE']
        missing_images = []
        for i, row in mp_rage_df.iterrows():
            subject_date_match = [s for s in image_list if row['SCANDATE'] in s and row['SUBJECT'] in s
                                  and str(row['SERIESID']) in s and str(row['IMAGEUID']) in s]
            print(len(subject_date_match))
            if len(subject_date_match) == 0:
                missing_images.append(row)
        print(missing_images)
