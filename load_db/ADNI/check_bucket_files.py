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
        adni_merge_df = pd.read_csv('../../data/ADNI_metadata/ADNIMERGE.csv').drop_duplicates(subset=['RID'],
                                                                                              keep='first')
        adni_merge_subject_list = list(set(adni_merge_df['RID'].to_list()))
        adni_demographics_df = pd.read_csv('../../data/ADNI_metadata/PTDEMOG.csv')
        adni_demographics_subject_list = list(set(adni_demographics_df['RID'].to_list()))
        cos_client = ibm_boto3.resource('s3',
                                        ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                        ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                        ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                        config=Config(signature_version='oauth'),
                                        endpoint_url=app.config['SERVICE_ENDPOINT'])
        subject_list = []
        bucket = cos_client.Bucket(app.config['BUCKET'])
        for i in bucket.objects.all():
            if i.size != 0 and 'ADNI' in i.key:
                try:
                    subject_list.append(int(i.key.split('ADNI_')[1].split('_')[2]))
                except:
                    continue
        unique_subject_list = list(set(subject_list))
        # yields the elements in `unique_subject_list` that are NOT in `adni_merge_df`
        unique_subject_list_notin_adni_merge_df = np.setdiff1d(unique_subject_list,adni_merge_subject_list)
        unique_subject_list_notin_adni_demographics_df = np.setdiff1d(unique_subject_list, adni_demographics_subject_list)
        adni_merge_df_notin_unique_subject_list = np.setdiff1d(adni_merge_subject_list, unique_subject_list)
        adni_demographics_df_notin_unique_subject_list = np.setdiff1d(adni_demographics_subject_list,unique_subject_list)
        print('Subjects that have images but are not part of ADNIMERGE csv:'+str(len(unique_subject_list_notin_adni_merge_df)))
        print('Subjects that have images but are not part of ADNI DEMOGRAPHICS csv:'+str(len(unique_subject_list_notin_adni_demographics_df)))
        print('Subjects present in ADNIMERGE csv that not have images:' + str(
            len(adni_merge_df_notin_unique_subject_list)))
        print('Subjects present in ADNI DEMOGRAPHICS csv that not have images:' + str(
            len(adni_demographics_df_notin_unique_subject_list)))
        with open('../../data/ADNI_metadata/ADNISUBJECTIMAGES.pkl', 'wb') as f:
            pickle.dump(unique_subject_list, f)
