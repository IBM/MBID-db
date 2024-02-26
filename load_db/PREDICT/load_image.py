#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 15 17:33:34 2022

@author: ecastrow
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

    # Create logger
    logger = logging.getLogger(__name__)  
    logger.setLevel(logging.INFO)

    # create file and console loggers with different log levels
    fh = logging.FileHandler('debug_image.log')
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
    
    # Load imaging and clinical information (visit & subjid as str)
    image_df = pd.read_csv('../../data/PREDICT-HD_metadata/imaging.csv',
                           dtype={'subjid': str, 'visit': str})
    visit_df = pd.read_csv('../../data/PREDICT-HD_metadata/longitudinal.csv',
                           dtype={'subjid': str, 'visit': str})
    baseline_df = pd.read_csv('../../data/PREDICT-HD_metadata/baseline.csv',
                              dtype={'subjid': str, 'visit': str})
    
    image_df['subjid'] = image_df['subjid'].map('{:0>6}'.format)
    visit_df['subjid'] = visit_df['subjid'].map('{:0>6}'.format)
    baseline_df['subjid'] = baseline_df['subjid'].map('{:0>6}'.format)
    
    tesla_map = {15: 'one_and_a_half', 30: 'three'}
    
    # Matias' initial setup
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))

    with app.app_context():
        study_name = "PREDICT-HD"
        dataset_object = SourceDataset.query.filter(SourceDataset.designation
                                                    == study_name).first()
        cos_client =\
            ibm_boto3.resource('s3',
                               ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                               ibm_service_instance_id=\
                                   app.config['COS_CREDENTIALS']['resource_instance_id'],
                               ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                               config=Config(signature_version='oauth'),
                               endpoint_url=app.config['SERVICE_ENDPOINT'])
    
        bucket = cos_client.Bucket(app.config['BUCKET'])
        
        # Iterate through imaging files uploaded to COS to populate DB
        old_subj_vst = 'abc_123'
        for i in bucket.objects.all():
            if (i.key.startswith(study_name)) and (i.size != 0)\
                and ('nii.gz' in i.key):

                # Get imaging filenames
                mri_base_fn = i.key.split('/')[-1]
                
                # Retrieve 'image' fields
                img_row =\
                    image_df[image_df['full_fname'].str.contains(mri_base_fn,
                                                                 case=False)]
                img_row = img_row.squeeze()

                # If image not in list of acceptable imaging files, skip it
                if img_row.empty:
                    logger.warning('Image {} may not be valid. Skipping it'.\
                                   format(mri_base_fn))
                    continue
                
                # Make sure that *days* (int64) is converted to plain int
                subject_external_id = img_row['subjid']
                visit_external_id = img_row['visit']
                img_days_since_baseline = int(img_row['visdy'])
                modality = img_row['modality']
                new_subj_visit =\
                    f'{subject_external_id}_{visit_external_id:0>2}'
                
                # Retrieve clinical values only if a new subject/visit
                if new_subj_visit != old_subj_vst:
                    old_subj_vst = new_subj_visit
                
                    # Retrieve 'visit' fields
                    vst_row =\
                        visit_df[(visit_df['subjid'] == subject_external_id) &
                                 (visit_df['visit'] == visit_external_id)]
                    vst_row = vst_row.squeeze()
                    
                    base_row = baseline_df[baseline_df['subjid'] ==
                                           subject_external_id]
                    base_row = base_row.squeeze()
                    
                    # If no clinical information for that visit, raise potential error 
                    if vst_row.empty:
                        logger.error('No clinical data for Visit {0} of '
                                     'Subject {1}. Skipping it'.\
                                     format(visit_external_id,
                                            subject_external_id))
                        continue
    
                    # replace NaN entries with SQL-compatible None
                    vst_row = vst_row.where(pd.notnull(vst_row), None)
                    base_row = base_row.where(pd.notnull(base_row), None)
                    
                    condition = get_predict_condition(vst_row['group'])
                    if condition:
                        condition_id = condition.id
                    else:
                        condition_id = None
    
                    # Make sure that *days* (int64) is converted to plain int
                    symptoms = {}
                    symptoms['tms'] = vst_row['motscore']
                    symptoms['sdmt'] = vst_row['sdmt1']
                    symptoms['tfc'] = vst_row['tfcscore']
                    symptoms['cap'] = vst_row['CAP']
                    symptoms['cag'] = base_row['CAG']
                    vst_days_since_baseline = int(vst_row['visdy'])

                    # Make sure that *bmi* (float64) is converted to plain float
                    if not vst_row.isnull()['bmi']:
                        bmi = float(vst_row['bmi'])
                    else:
                        bmi = None
                
                    # Query subject in DB              
                    existing_subject =\
                        Subject.query.filter(Subject.external_id
                                             == subject_external_id,
                                             Subject.source_dataset_id
                                             == dataset_object.id).first()
                
                    # Proceed with DB modification for this image only if subject in DB
                    if existing_subject:
                        existing_visit =\
                            Visit.query.filter(Visit.external_id
                                               == visit_external_id,
                                               Visit.subject_id
                                               == existing_subject.id,
                                               Visit.source_dataset_id
                                               == dataset_object.id).first()
                        if existing_visit:
                            existing_visit.days_since_baseline =\
                                vst_days_since_baseline
                            existing_visit.symptoms = symptoms
                            existing_visit.condition_id = condition_id
                            existing_visit.bmi = bmi
                            db.session.merge(existing_visit)
                            logger.info('Updating Visit {0} of Subject {1}'\
                                        .format(visit_external_id,
                                                subject_external_id))
                        else:
                            existing_visit =\
                                Visit(external_id=visit_external_id,
                                      subject_id=existing_subject.id,
                                      source_dataset_id=dataset_object.id,
                                      days_since_baseline=\
                                          vst_days_since_baseline,
                                      symptoms=symptoms,
                                      condition_id=condition_id,
                                      bmi=bmi)
                            logger.info('Creating Visit {0} of Subject {1}'\
                                        .format(visit_external_id,
                                                subject_external_id))
                            db.session.add(existing_visit)
                        db.session.commit()

                    else:
                        logger.error('Subject {} not in DB'.format(subject_external_id))
                        continue

                # Retrieve scanner info
                site_code = str(int(img_row['siteid']))
                scan_brand = img_row['manufacturer']
                tr = int(np.floor(img_row['tr']))
                                    
                scan_json = {'Manufacturer': scan_brand,
                             'ManufacturersModelName':
                                 img_row['model'],
                             'RepetitionTime': img_row['tr'],
                             'EchoTime': img_row['te'],
                             'FlipAngle': int(img_row['flip']),
                             'MagneticFieldStrength':
                                 img_row['field_st']/10}
                
                existing_scanner =\
                    Scanner.query.filter(Scanner.brand == scan_brand,
                                         Scanner.model == img_row['model'],
                                         Scanner.source_id == site_code,
                                         Scanner.source_dataset_id ==
                                         dataset_object.id).first()
                

                if existing_scanner:
                    existing_scanner.teslas =\
                        tesla_map[img_row['field_st']]
                    db.session.merge(existing_scanner)
                else:
                    existing_scanner =\
                        Scanner(brand=scan_brand,
                                model=img_row['model'],
                                source_dataset_id=dataset_object.id,
                                teslas=tesla_map[img_row['field_st']],
                                source_id=site_code)
                    db.session.add(existing_scanner)
                db.session.commit()
                      
                # Upload image info in DB
                existing_image =\
                    Image.query.filter(Image.visit_id ==
                                       existing_visit.id,
                                       Image.subject_id ==
                                       existing_subject.id,
                                       Image.source_dataset_id
                                       == dataset_object.id,
                                       Image.image_path
                                       == i.key).first()
                if existing_image:
                    existing_image.image_path = i.key
                    existing_image.file_size = i.size
                    existing_image.type = modality
                    existing_image.days_since_baseline =\
                        img_days_since_baseline
                    existing_image.metadata_json = scan_json
                    existing_image.scanner_id = existing_scanner.id
                    db.session.merge(existing_image)
                    logger.info('Updating Image {} in DB'\
                                .format(mri_base_fn))
                else:
                    image_db = Image(visit_id=existing_visit.id,
                                     subject_id=existing_subject.id,
                                     source_dataset_id=dataset_object.id,
                                     image_path=i.key, file_size=i.size,
                                     type=modality,
                                     days_since_baseline=\
                                         img_days_since_baseline,
                                     metadata_json=scan_json,
                                     scanner_id=existing_scanner.id)
                    db.session.add(image_db)
                    logger.info('Creating Image {} in DB'\
                                .format(mri_base_fn))
                db.session.commit()            
