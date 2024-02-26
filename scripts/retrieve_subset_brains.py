#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 20:03:05 2023

Script to download a subset of images from COS (only one image per subject). 

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
studies = ['SALD', 'OASIS', 'HCP-Aging', 'PREDICT-HD']
store_path = '/data/eduardo/temp'
nmb_img = 5     # Number of brains to download from each study


# Default setup to load a database; load SQL DB credentials
current_app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
engine = db.create_engine(current_app.config['SQLALCHEMY_DATABASE_URI'])

connection = engine.connect()
metadata = db.MetaData()

# Load tables of interest; define studies to look into
study_table = db.Table('source_dataset', metadata, autoload=True,
                        autoload_with=engine)
image_table = db.Table('image', metadata, autoload=True, autoload_with=engine)

# Retrieve DB values assigned to the analyzed studies
study_df = pd.read_sql('SELECT id, designation FROM source_dataset',
                       connection)
study_df = study_df[study_df.designation.isin(studies)]

# Retrieve data from image table (only studies of interest)
str_studies = study_df['id'].values
str_studies = [str(std) for std in str_studies]
str_studies = '(' + ','.join(str_studies) + ')'
img_df = pd.read_sql('SELECT subject_id, type, source_dataset_id, '
                     'image_path FROM image WHERE '
                     f'source_dataset_id IN {str_studies}', connection)
img_df = img_df[img_df.type == 'T1']

study_map = dict(zip(study_df.id, study_df.designation))
img_df['study'] = img_df['source_dataset_id'].map(study_map)
del img_df['source_dataset_id']

# Retrieve 5 images per subject, per study
img_df = img_df.groupby('subject_id').first().reset_index()
img_df = img_df.groupby('study').head(nmb_img).reset_index(drop=True)
img_df['local_fn'] = img_df.apply(lambda x:
                                  '_'.join([x['study'], str(x['subject_id'])])\
                                  + '.nii.gz', axis=1)

# Copy those images on a temporary location
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
