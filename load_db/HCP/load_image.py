from botocore.client import Config
import ibm_boto3
from io import StringIO
import types
import os
import pandas as pd
import sys
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner
from load_file.utils import read_s3_contents
from pathlib import Path


if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'Human connectome project').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'HCP1200'
        T3_metadata = Path(__file__).parent / '../../data/HCP1200_metadata/T3.json'
        with open(T3_metadata) as t3_json_file:
            T3_metadata_json = json.load(t3_json_file)
        image_metadata_json = T3_metadata_json
        existing_scanner = Scanner.query.filter(Scanner.brand == T3_metadata_json['Manufacturer'],
                                                Scanner.model == T3_metadata_json['ManufacturersModelName'],
                                                Scanner.source_dataset_id == dataset_object.id).first()
        scanner_teslas = None
        if T3_metadata_json['MagneticFieldStrength'] == '3':
            scanner_teslas = 'three'
        elif T3_metadata_json['MagneticFieldStrength'] == '1.5':
            scanner_teslas = 'one_and_a_half'
        if existing_scanner:
            existing_scanner.teslas = scanner_teslas
            db.session.merge(existing_scanner)
        else:
            existing_scanner = Scanner(brand=T3_metadata_json['Manufacturer'],
                                       model=T3_metadata_json['ManufacturersModelName'],
                                       source_dataset_id=dataset_object.id, teslas=scanner_teslas)
            db.session.add(existing_scanner)
        db.session.commit()

        subjects_info_df = pd.read_csv(Path(__file__).parent / '../../data/HCP1200_metadata/Behavioral_Individual_Subject_Measures.csv',
                                       dtype={'Subject': str},
                                       usecols=['Subject', 'PMAT24_A_CR', 'PMAT24_A_RTCR'])
        subjects_info_df.set_index('Subject', inplace=True)
        restricted_df = pd.read_csv(Path(__file__).parent / '../../data/HCP1200_metadata/RESTRICTED_ecastrow_8_12_2021_16_51_2.csv',
                                    dtype={'Subject': str},
                                    usecols=['Subject', 'BMI', 'SSAGA_Educ',])
        restricted_df.set_index('Subject', inplace=True)
        for i in bucket.objects.all():
            if (i.key.startswith(prefix)) and (i.size != 0) and ('nii' in i.key.split('.')):
                subject_external_id = i.key.split('/')[1]
                type = None
                if 'T1w' in i.key and 'MPR1.nii' in i.key:
                    type = 'T1'
                else:
                    continue
                # Visit parameters:
                visit_external_id = '1' # HCP is not longitudinal
                visit_days_since_baseline = 0
                existing_subject = Subject.query.filter(Subject.external_id == subject_external_id,
                                                        Subject.source_dataset_id == dataset_object.id).first()
                if existing_subject:
                    existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                        Visit.subject_id == existing_subject.id,
                                                        Visit.source_dataset_id == dataset_object.id).first()

                    # Make symptom dict:
                    sym_res = restricted_df.loc[subject_external_id]
                    sym_sub = subjects_info_df.loc[subject_external_id]
                    sym_series = pd.concat([sym_sub, sym_res])
                    sym_name_maps = {'BMI': 'bmi',
                                     'SSAGA_Educ': 'education_yrs',
                                     'PMAT24_A_CR': 'fluid_intl_matrix_n_correct',
                                     'PMAT24_A_RTCR': 'fluid_intl_matrix_RT'}
                    symptoms = {v: None for v in sym_name_maps.values()}
                    # Make sure there are no nans in symptoms:
                    for key, val in sym_name_maps.items():
                        if not pd.isnull(sym_series[key]):
                            symptoms[val] = sym_series[key]

                    if existing_visit:
                        existing_visit.days_since_baseline = visit_days_since_baseline
                        existing_visit.symptoms = symptoms
                        db.session.merge(existing_visit)
                    else:
                        existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                               source_dataset_id=dataset_object.id,
                                               days_since_baseline=visit_days_since_baseline,
                                               symptoms=symptoms)
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
                        existing_image.days_since_baseline = visit_days_since_baseline
                        existing_image.metadata_json = image_metadata_json
                        existing_image.scanner_id = existing_scanner.id
                        db.session.merge(existing_image)
                    else:
                        image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                         source_dataset_id=dataset_object.id,
                                         image_path=i.key, file_size=i.size,
                                         type=type, metadata_json=image_metadata_json,
                                         days_since_baseline=visit_days_since_baseline,
                                         scanner_id=existing_scanner.id)
                        db.session.add(image_db)
                    db.session.commit()
                else:
                    print('ERROR: missing subject id ' + subject_external_id)
