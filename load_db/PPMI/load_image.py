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
from load_db.PPMI import get_ppmi_condition


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
    
    # Matias' initial setup
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        PPMI_study_name = "Parkinson's Progression Markers Initiative"
        dataset_object = SourceDataset.query.filter(SourceDataset.designation
                                                    == PPMI_study_name).first()
        cos_client =\
            ibm_boto3.resource('s3',
                               ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                               ibm_service_instance_id=\
                                   app.config['COS_CREDENTIALS']['resource_instance_id'],
                               ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                               config=Config(signature_version='oauth'),
                               endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])

        # Load imaging and clinical information dataframes (visit & subjid as str)
        image_df = pd.read_csv('../../data/PPMI_metadata/img_db_ready.csv',
                               dtype={'PATNO': str, 'visit': str})
        visit_df = pd.read_csv('../../data/PPMI_metadata/longit_clinical_info.csv',
                               dtype={'PATNO': str, 'visit': str})

        # Specify addl imaging information (metadata and directory prefix in COS)
        prefix = 'PPMI'
        T1_json_fn = '../../data/PPMI_metadata/T1.json'
        DTI_json_fn = '../../data/PPMI_metadata/DTI.json'
        func_json_fn = '../../data/PPMI_metadata/fMRI.json'        
        dti_params = np.load('../../data/PPMI_metadata/DTI_extra_params.npz',
                             allow_pickle=True)
        bvec = dti_params['bvec'].squeeze()
        bvec = [list(bv) for bv in bvec]
        bval = list(dti_params['bval'])
        
        # Load actual imaging metadata and add missing DTI fields
        with open(DTI_json_fn, 'r') as f:
            DTI_json = json.load(f)
            DTI_json['bvec'] = bvec
            DTI_json['bval'] = bval

        with open(T1_json_fn, 'r') as f:
            T1_json = json.load(f)

        with open(func_json_fn, 'r') as f:
            func_json = json.load(f)

        img_meta_dict = {'T1': T1_json, 'DWI': DTI_json, 'rsfMRI': func_json}
        
        # Check scanner info
        # -> Note: using T1 only because all are 3T and scanner object has few fields
        existing_scanner =\
            Scanner.query.filter(Scanner.brand == T1_json['Manufacturer'],
                                 Scanner.model == T1_json['ManufacturersModelName'],
                                 Scanner.source_dataset_id == dataset_object.id).first()
        scanner_teslas = None
        if T1_json['MagneticFieldStrength'] == 3:
            scanner_teslas = 'three'
        elif T1_json['MagneticFieldStrength'] == 1.5:
            scanner_teslas = 'one_and_a_half'
        if existing_scanner:
            existing_scanner.teslas = scanner_teslas
            db.session.merge(existing_scanner)
        else:
            existing_scanner =\
                Scanner(brand=T1_json['Manufacturer'],
                        model=T1_json['ManufacturersModelName'],
                        source_dataset_id=dataset_object.id,
                        teslas=scanner_teslas)
            db.session.add(existing_scanner)
        db.session.commit()
        # Notes:
            # 1. Studies don't have single scanners. Usually several sites and scanners
            # 2. Differences in scanners among modalities (disregarding both issues here)
        
        # Iterate through imaging files uploaded to COS to populate DB
        old_subjid = 'abcd'
        for i in bucket.objects.all():
            if (i.key.startswith(prefix)) and (i.size != 0) and ('nii' in i.key.split('.')):

                # Get imaging filenames; leave filename as *.nii (discard .gz part)
                mri_base_fn = i.key.split('/')[-1]
                mri_base_fn = '.'.join(mri_base_fn.split('.')[:-1])
                
                # Retrieve 'image' fields
                img_row = image_df[image_df['fname'].str.contains(mri_base_fn,
                                                                  case=False)]
                img_row = img_row.squeeze()

                # If image not in list of acceptable imaging files, skip it
                if img_row.empty:
                    logger.warning('Image {} may not be valid. Skipping it'.\
                                   format(mri_base_fn))
                    continue

                # Make sure that *days* (int64) is converted to plain int
                subject_external_id = img_row['PATNO']
                visit_external_id = img_row['visit']
                img_days_since_baseline = int(img_row['days_since_baseline'])
                modality = img_row['modality']
                img_metadata_json = img_meta_dict[modality]

                # Retrieve 'visit' fields
                vst_row = visit_df[(visit_df['PATNO'] == subject_external_id) &
                                   (visit_df['visit'] == visit_external_id)]

                # If no clinical information for that visit, raise potential error 
                if vst_row.empty:
                    logger.error('No clinical data for Visit {0} of '
                                 'Subject {1}. Skipping it'.\
                                 format(visit_external_id,
                                        subject_external_id))
                    continue
                
                # If entries for ON and OFF states, merge values and take single row
                if vst_row.shape[0] > 1:
                    vst_row = vst_row.ffill().bfill().iloc[0]
                
                vst_row = vst_row.squeeze()

                # replace NaN entries with SQL-compatible None
                vst_row = vst_row.where(pd.notnull(vst_row), None)
                
                condition = get_ppmi_condition(vst_row['condition'])
                if condition:
                    condition_id = condition.id
                else:
                    condition_id = None

                # Make sure that *days* (int64) is converted to plain int
                updrs_vars = list(vst_row.index[
                    vst_row.index.str.contains('updrs')].values)
                symptoms = {uvar: vst_row[uvar] for uvar in updrs_vars}
                symptoms['moca'] = vst_row['moca_total']
                symptoms['sdmt'] = vst_row['sdmt']
                vst_days_since_baseline = int(vst_row['days_since_baseline'])

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

                # If subject in DB, proceed and query visit associated to image in COS
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
                        existing_visit = Visit(external_id=visit_external_id,
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
                    
                    # Once the visit is available in DB, upload image info there too
                    existing_image = Image.query.filter(Image.visit_id ==
                                                        existing_visit.id,
                                                        Image.subject_id ==
                                                        existing_subject.id,
                                                        Image.source_dataset_id
                                                        == dataset_object.id,
                                                        Image.image_path
                                                        == i.key).first()
                    if existing_image:
                        existing_image.file_size = i.size
                        existing_image.type = modality
                        existing_image.days_since_baseline =\
                            img_days_since_baseline
                        existing_image.metadata_json = img_metadata_json
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
                                         metadata_json=img_metadata_json,
                                         days_since_baseline=\
                                             img_days_since_baseline,
                                         scanner_id=existing_scanner.id)
                        db.session.add(image_db)
                        logger.info('Creating Image {} in DB'\
                                    .format(mri_base_fn))
                    db.session.commit()
                else:
                    if subject_external_id != old_subjid:
                        old_subjid = subject_external_id
                        logger.error('Subject {} not in DB'.format(subject_external_id))
