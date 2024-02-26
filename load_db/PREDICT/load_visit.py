#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 18:09:58 2022

@author: ecastrow
"""
import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import logging
from app import create_app, db
from app.models import Subject, SourceDataset, Visit
from load_db.PREDICT import get_predict_condition
from config.globals import ENVIRONMENT


if __name__ == '__main__':

    # Create logger
    logger = logging.getLogger(__name__)  
    logger.setLevel(logging.INFO)

    # create file and console loggers with different log levels
    fh = logging.FileHandler('debug_visit.log')
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
        study_name = 'PREDICT-HD'
        dataset_object = SourceDataset.query.filter(SourceDataset.designation
                                                    == study_name).first()
        
        # Load clinical information dataframe (visit & subjid as str))
        visit_df = pd.read_csv('../../data/PREDICT-HD_metadata/longitudinal.csv',
                               dtype={'subjid': str, 'visit': str})
        baseline_df = pd.read_csv('../../data/PREDICT-HD_metadata/baseline.csv',
                                  dtype={'subjid': str, 'visit': str})
        
        visit_df['subjid'] = visit_df['subjid'].map('{:0>6}'.format)
        baseline_df['subjid'] = baseline_df['subjid'].map('{:0>6}'.format)
        
        # Iterate through subj/visit pair entries in clinical info df
        old_subjid = 'abcd'
        subj_vst_df = visit_df[['subjid', 'visit']].copy()
        subj_vst_df.drop_duplicates(inplace=True)
        
        for idx, (subjid, vst) in subj_vst_df.iterrows():
            
            # Retrieve matching fields
            vst_row = visit_df[(visit_df['subjid'] == subjid) &
                               (visit_df['visit'] == vst)]
            vst_row = vst_row.squeeze()
            
            base_row = baseline_df[baseline_df['subjid'] == subjid]
            base_row = base_row.squeeze()
            
            # replace NaN entries with SQL-compatible None
            vst_row = vst_row.where(pd.notnull(vst_row), None)
            base_row = base_row.where(pd.notnull(base_row), None)
            
            # Query subject in DB
            existing_subject =\
                Subject.query.filter(Subject.external_id == subjid,
                                     Subject.source_dataset_id
                                     == dataset_object.id).first()
            
            # If subject in DB, proceed with visit info
            if existing_subject:
                condition = get_predict_condition(vst_row['group'])
                if condition:
                    condition_id = condition.id
                else:
                    condition_id = None
                
                # Make sure that *bmi* (float64) is converted to plain float
                if not vst_row.isnull()['bmi']:
                    bmi = float(vst_row['bmi'])
                else:
                    bmi = None
                
                # clinical variables per se (on top of condition)
                symptoms = {}
                symptoms['tms'] = vst_row['motscore']
                symptoms['sdmt'] = vst_row['sdmt1']
                symptoms['tfc'] = vst_row['tfcscore']
                symptoms['cap'] = vst_row['CAP']
                symptoms['cag'] = base_row['CAG']
                visit_external_id = vst_row['visit']
                vst_days_since_baseline = int(vst_row['visdy'])
                
                existing_visit =\
                    Visit.query.filter(Visit.external_id
                                       == visit_external_id,
                                       Visit.subject_id
                                       == existing_subject.id,
                                       Visit.source_dataset_id
                                       == dataset_object.id).first()
                
                if existing_visit:
                    existing_visit.days_since_baseline = vst_days_since_baseline
                    existing_visit.symptoms = symptoms
                    existing_visit.condition_id = condition_id
                    existing_visit.bmi = bmi
                    db.session.merge(existing_visit)
                    logger.info('Updating Visit {0} of Subject {1}'\
                                .format(visit_external_id,
                                        existing_subject.external_id))
                else:
                    existing_visit = Visit(external_id=visit_external_id,
                                           subject_id=existing_subject.id,
                                           source_dataset_id=dataset_object.id,
                                           days_since_baseline=vst_days_since_baseline,
                                           symptoms=symptoms,
                                           condition_id=condition_id,
                                           bmi=bmi)
                    logger.info('Creating Visit {0} of Subject {1}'\
                                .format(visit_external_id,
                                        existing_subject.external_id))
                    db.session.add(existing_visit)
                db.session.commit()
            else:
                if subjid != old_subjid:
                    old_subjid = subjid
                    logger.error('Subject {} not in DB'.format(subjid))
