from botocore.client import Config
import ibm_boto3
from io import StringIO
import types
import os
import pandas as pd
import sys
import json
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import pdb
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner, Condition
from load_file.utils import read_s3_contents
from load_db.OASIS import get_oasis_dx
pd.options.mode.chained_assignment = None



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        clinical_data_df = pd.read_csv('../../data/OASIS_metadata/clinical_data.csv')
        mr_sessions_df = pd.read_csv('../../data/OASIS_metadata/MR_sessions.csv')
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'OASIS').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'OASIS'

        for i in bucket.objects.all():
            if i.key.startswith(prefix) and i.size != 0 and i.key.endswith('nii.gz'):
                subject_external_id = i.key.split('sub-')[1].split('/')[0]
                clinical_data_subject = clinical_data_df[clinical_data_df['Subject'] == subject_external_id]
                clinical_data_subject.loc[:,'days_since_baseline'] = clinical_data_subject.copy()['ADRC_ADRCCLINICALDATA ID'].str[-4:].astype(int)
                mr_sessions_subject = mr_sessions_df[mr_sessions_df['Subject'] == subject_external_id]
                image_day = int(''.join(c for c in i.key.split('ses-')[-1].split('_')[0] if c.isdigit()))
                past_clinical_data = clinical_data_subject[clinical_data_subject['days_since_baseline']<=image_day]
                closest_past_clinical_data = past_clinical_data.iloc[(past_clinical_data['days_since_baseline']-image_day).abs().argsort()[:1]]
                visit_external_id = closest_past_clinical_data['ADRC_ADRCCLINICALDATA ID'].values[0]
                visit_days_since_baseline = int(closest_past_clinical_data['days_since_baseline'].values[0])
                visit_dx1 = str(closest_past_clinical_data['dx1'].values[0]).lower()
                visit_cdr = closest_past_clinical_data['cdr'].values[0]
                symptoms = dict()
                if pd.notnull(closest_past_clinical_data['mmse'].values[0]):
                    symptoms['mmse'] = closest_past_clinical_data['mmse'].values[0]
                else:
                    symptoms['mmse'] = None
                symptoms['cdr'] = visit_cdr
                bmi = None
                if closest_past_clinical_data['height'].values[0] and closest_past_clinical_data['weight'].values[0]\
                        and not np.isnan(closest_past_clinical_data['height'].values[0])\
                        and not np.isnan(closest_past_clinical_data['weight'].values[0]):
                    bmi = (closest_past_clinical_data['height'].values[0]/closest_past_clinical_data['weight'].values[0]**2)*703
                type = None
                if 'func' in i.key and ('task' in i.key or 'asl' in i.key):
                    continue
                elif 'anat' in i.key and ('T2' in i.key or 'FLAIR' in i.key or 'angio' in i.key):
                    continue
                elif 'swi' in i.key:
                    continue
                elif 'func' in i.key and 'resting_bold' in i.key:
                    type = 'rsfMRI'
                elif 'fmap' in i.key and 'fieldmap' in i.key:
                    type = 'FieldMap'
                elif 'anat' in i.key and 'T1' in i.key:
                    type = 'T1'
                elif 'dwi' in i.key:
                    type = 'DWI'
                image_metadata = i.key.replace('nii.gz','json')
                try:
                    image_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'],
                                                                          image_metadata)
                except cos_client.meta.client.exceptions.NoSuchKey:
                    print("Can't find image metadata for file "+image_metadata)
                    continue

                image_metadata_json = json.loads(image_metadata_content.decode("utf-8"))
                if 'DeviceSerialNumber' in image_metadata_json:
                    serial_number = image_metadata_json['DeviceSerialNumber']
                else:
                    serial_number = None
                existing_scanner = Scanner.query.filter(Scanner.brand == image_metadata_json['Manufacturer'],
                                                        Scanner.model == image_metadata_json['ManufacturersModelName'],
                                                        Scanner.source_id == serial_number,
                                                        Scanner.source_dataset_id == dataset_object.id).first()


                scanner_teslas = None
                if image_metadata_json['MagneticFieldStrength'] == '3' or image_metadata_json['MagneticFieldStrength'] == 3:
                    scanner_teslas = 'three'
                elif image_metadata_json['MagneticFieldStrength'] == '1.5' or image_metadata_json['MagneticFieldStrength'] == 1.5:
                    scanner_teslas = 'one_and_a_half'
                if existing_scanner:
                    existing_scanner.teslas = scanner_teslas
                    db.session.merge(existing_scanner)
                else:
                    existing_scanner = Scanner(brand=image_metadata_json['Manufacturer'],
                                               model=image_metadata_json['ManufacturersModelName'],
                                               source_id = serial_number,
                                               source_dataset_id=dataset_object.id, teslas=scanner_teslas)
                    db.session.add(existing_scanner)
                db.session.commit()

                existing_subject = Subject.query.filter(Subject.external_id == subject_external_id,
                                                    Subject.source_dataset_id == dataset_object.id).first()
                if existing_subject:
                    existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                        Visit.subject_id == existing_subject.id,
                                                            Visit.source_dataset_id == dataset_object.id).first()
                    condition = get_oasis_dx(visit_cdr, visit_dx1)
                    if condition:
                        condition_id = condition.id
                    else:
                        condition_id = None
                    if existing_visit:
                        existing_visit.days_since_baseline = visit_days_since_baseline
                        existing_visit.symptoms = symptoms
                        existing_visit.condition_id = condition_id
                        existing_visit.bmi = bmi
                        db.session.merge(existing_visit)
                    else:
                        existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                             source_dataset_id=dataset_object.id, days_since_baseline=visit_days_since_baseline,
                                               symptoms=symptoms, condition_id=condition_id,
                                               bmi=bmi)
                        db.session.add(existing_visit)
                    db.session.commit()
                    existing_image = Image.query.filter(Image.visit_id == existing_visit.id,
                                                        Image.subject_id == existing_subject.id,
                                                        Image.source_dataset_id == dataset_object.id,
                                                        Image.image_path == i.key).first()
                    if existing_image:
                        existing_image.image_path = i.key
                        existing_image.file_size = i.size
                        existing_image.type = type
                        existing_image.days_since_baseline = image_day
                        existing_image.metadata_json = image_metadata_json
                        existing_image.scanner_id = existing_scanner.id
                        db.session.merge(existing_image)
                    else:
                        image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                             source_dataset_id=dataset_object.id,
                                             image_path=i.key, file_size=i.size,
                                         type=type, metadata_json=image_metadata_json,
                                         days_since_baseline=image_day,
                                         scanner_id=existing_scanner.id)
                        db.session.add(image_db)
                    db.session.commit()
                else:
                    print('ERROR: missing subject id '+subject_external_id)