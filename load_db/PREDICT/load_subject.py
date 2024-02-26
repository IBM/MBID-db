#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 00:01:51 2022

@author: Eduardo Castro
"""
from botocore.client import Config
import ibm_boto3
import os
import pandas as pd
import numpy as np
import sys
import json
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner
from load_db.PREDICT import get_predict_condition


if __name__ == '__main__':
    # Mapping of PREDICT table sex values to DB defined ones
    sex_map = {'m': 'male', 'f': 'female'}
    
    # Create logger
    logger = logging.getLogger(__name__)  
    logger.setLevel(logging.INFO)

    # create file and console loggers with different log levels
    fh = logging.FileHandler('debug_subject.log')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    # Matias' initial setup
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        # Load baseline info of subjects (generated in predict-hd_tables_retrieval.py)
        study_name = 'PREDICT-HD'
        subjects_info_df = pd.read_csv('../../data/PREDICT-HD_metadata/'
                                       'baseline.csv', dtype={'subjid': str})
        subjects_info_df['subjid'] =\
            subjects_info_df['subjid'].map('{:0>6}'.format)
        dataset_object = SourceDataset.query.filter(SourceDataset.designation
                                                    == study_name).first()
        
        # Iterate through entries of subject dataframe; populate database
        for index, subject_row in subjects_info_df.iterrows():
            print('Subject #{0} of {1}'.format(index+1,
                                               subjects_info_df.shape[0]))
            existing_subject =\
                Subject.query.filter(Subject.external_id == subject_row.subjid,
                                     Subject.source_dataset_id ==
                                     dataset_object.id).first()
                
            # Retrieve condition representation in DB
            condition = get_predict_condition(subject_row['group'])
            if condition:
                condition_id = condition.id
            else:
                condition_id = None

            if existing_subject:
                existing_subject.external_id = subject_row.subjid
                existing_subject.gender = sex_map[subject_row.sex]
                existing_subject.race = subject_row.race_nih
                existing_subject.condition_id = condition_id
                existing_subject.age_at_baseline = subject_row.age
                db.session.merge(existing_subject)
                logger.info('Updating Subject {} info'\
                            .format(subject_row.subjid))
            else:
                subject_db = Subject(external_id=subject_row.subjid,
                                     gender=sex_map[subject_row.sex],
                                     race=subject_row.race_nih,
                                     condition_id=condition_id,                                     
                                     age_at_baseline=subject_row.age,
                                     source_dataset_id=dataset_object.id)
                logger.info('Creating Subject {} info'\
                            .format(subject_row.subjid))
                db.session.add(subject_db)
            db.session.commit()