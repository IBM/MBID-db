#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 9 2023

Script to test nonlinear registration of OpenPain after updating the DB
and uploading the images to COS.

@author: Eduardo Castro
"""

import ibm_boto3
from botocore.client import Config
from os.path import join as opj
import sqlalchemy as db
import pandas as pd
import os
from os import path
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import create_app
from config.globals import ENVIRONMENT


"""
INPUTS
"""
studies = ['OpenPain']
store_path = '/data/eduardo/temp/openpain_test'
nmb_img = 5     # Number of brains to download from each study


# Default setup to load a database; load SQL DB credentials
current_app = create_app('ibmcloud')
engine = db.create_engine(current_app.config['SQLALCHEMY_DATABASE_URI'])
connection = engine.connect()

# Retrieve assigned values to studies of interest in the DB
study_df = pd.read_sql('SELECT id, designation FROM source_dataset',
                       connection)
study_df = study_df[study_df.designation.isin(studies)]

str_studies = study_df['id'].values
str_studies = [str(std) for std in str_studies]
str_studies = '(' + ','.join(str_studies) + ')'

# Retrieve subset of subject IDs from studies of interest
subj_df = pd.read_sql('SELECT id, source_dataset_id FROM subject '
                      f'WHERE source_dataset_id IN {str_studies} '
                      f'LIMIT {nmb_img}', connection)
str_subj = [str(sub) for sub in subj_df['id'].values]
str_subj = '(' + ','.join(str_subj) + ')'

# Load imaging table for those subjects (T1W only)
img_df = pd.read_sql('SELECT id, subject_id, image_path, type, '
                     'source_dataset_id, preprocessed FROM image '
                     f'WHERE subject_id IN {str_subj}', connection)
img_df = img_df[img_df.type == 'T1']

# Retrieve single visit per subject; assign local filename
img_df = img_df.groupby('subject_id').first().reset_index()
img_df['local_fn'] = img_df.apply(lambda x: f'temp_{str(x.id)}_raw.nii.gz',
                                  axis=1)
str_img = [str(img_id) for img_id in img_df['id'].values]
str_img = '(' + ','.join(str_img) + ')'

# Load preprocessing table
pproc_dict = {1: 'reg_rigid', 34: 'reg_nlin', 35: 'warp_field'}
pproc_df = pd.read_sql('SELECT image_id, preprocess_task_id, file_type_id, '
                       'preprocess_file_path, preprocess_check_json FROM '
                       f'preprocess_task_file WHERE image_id IN {str_img}',
                       connection)
pproc_df = pproc_df[pproc_df.file_type_id.isin(list(pproc_df.keys()))]
pproc_df['local_fn'] = pproc_df.apply(lambda x: f'temp_{str(x.image_id)}'
                                  f'_{pproc_dict[x.file_type_id]}.nii.gz',
                                  axis=1)

# Store preprocessing info in csv file
pproc_df.to_csv(opj(store_path, 'preprocessing_info.csv'), index=False)

# Copy original and preprocessed images
with current_app.app_context():
    cos_client =\
        ibm_boto3.client('s3',
                         ibm_api_key_id=
                             current_app.config['COS_CREDENTIALS']['apikey'],
                         ibm_service_instance_id=
                             current_app.config['COS_CREDENTIALS']['resource_instance_id'],
                         ibm_auth_endpoint=
                             current_app.config['AUTH_ENDPOINT'],
                         config=Config(signature_version='oauth'),
                         endpoint_url=current_app.config['SERVICE_ENDPOINT'])
    image_bucket = current_app.config['BUCKET']
    
    if path.exists(store_path):
        for idx, row in img_df.iterrows():
            print(row.image_path)
            dest_fn = opj(store_path, row.local_fn)
            if not os.path.isfile(dest_fn):
                with open(dest_fn, 'wb') as f:
                    cos_client.download_fileobj(image_bucket,
                                                row.image_path, f)
        print('Done with raw images\n')
        
        for idx, row in pproc_df.iterrows():
            print(row.preprocess_file_path)
            dest_fn = opj(store_path, row.local_fn)
            if not os.path.isfile(dest_fn):
                with open(dest_fn, 'wb') as f:
                    cos_client.download_fileobj(image_bucket,
                                                row.preprocess_file_path, f)
        print('Done with preprocessed images')
