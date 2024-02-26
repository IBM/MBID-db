#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 19:53:10 2022

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
from load_db.PPMI import get_ppmi_condition
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
        PPMI_study_name = "Parkinson's Progression Markers Initiative"
        dataset_object = SourceDataset.query.filter(SourceDataset.designation
                                                    == PPMI_study_name).first()

        # Load clinical information dataframe (visit & subjid as str))
        visit_df = pd.read_csv('../../data/PPMI_metadata/longit_clinical_info.csv',
                               dtype={'PATNO': str, 'visit': str})

        # Iterate through subj/visit pair entries in clinical df
        old_subjid = 'abcd'
        subj_vst_df = visit_df[['PATNO', 'visit']].copy()
        subj_vst_df.drop_duplicates(inplace=True)
        for idx, (subjid, vst) in subj_vst_df.iterrows():
            # Retrieve matching fields fields
            vst_row = visit_df[(visit_df['PATNO'] == subjid) &
                               (visit_df['visit'] == vst)]
            
            # If entries for ON and OFF states, merge values and take single row            
            if vst_row.shape[0] > 1:
                vst_row = vst_row.ffill().bfill().iloc[0]

            vst_row = vst_row.squeeze()

            # replace NaN entries with SQL-compatible None
            vst_row = vst_row.where(pd.notnull(vst_row), None)

            # Query subject in DB
            existing_subject =\
                Subject.query.filter(Subject.external_id == subjid,
                                     Subject.source_dataset_id
                                     == dataset_object.id).first()

            # If subject in DB, proceed with visit info
            if existing_subject:
                condition = get_ppmi_condition(vst_row['condition'])
                if condition:
                    condition_id = condition.id
                else:
                    condition_id = None

                # Make sure that *days* (int64) is converted to plain int
                visit_external_id = vst_row['visit']
                vst_days_since_baseline = int(vst_row['days_since_baseline'])

                # Make sure that *bmi* (float64) is converted to plain float
                if not vst_row.isnull()['bmi']:
                    bmi = float(vst_row['bmi'])
                else:
                    bmi = None
                
                # clinical variables per se (on top of condition)
                updrs_vars = list(vst_row.index[
                    vst_row.index.str.contains('updrs')].values)
                symptoms = {uvar: vst_row[uvar] for uvar in updrs_vars}
                symptoms['moca'] = vst_row['moca_total']
                symptoms['sdmt'] = vst_row['sdmt']
                
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
