from botocore.client import Config
import ibm_boto3
from io import StringIO
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
import numpy as np

if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'HCP-Aging').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'HCA'
        seen_sites = []
        site_jsons = {}
        #######################################################################
        # Move this scanner stuff to inside the loop, each image has its json
        #######################################################################
        # T3_metadata = Path(__file__).parent / '../../data/HCP1200_metadata/T3.json'
        # with open(T3_metadata) as t3_json_file:
        #     T3_metadata_json = json.load(t3_json_file)
        # image_metadata_json = T3_metadata_json
        # existing_scanner = Scanner.query.filter(Scanner.brand == T3_metadata_json['Manufacturer'],
        #                                         Scanner.model == T3_metadata_json['ManufacturersModelName'],
        #                                         Scanner.source_dataset_id == dataset_object.id).first()
        # scanner_teslas = None
        # if T3_metadata_json['MagneticFieldStrength'] == '3':
        #     scanner_teslas = 'three'
        # elif T3_metadata_json['MagneticFieldStrength'] == '1.5':
        #     scanner_teslas = 'one_and_a_half'
        # if existing_scanner:
        #     existing_scanner.teslas = scanner_teslas
        #     db.session.merge(existing_scanner)
        # else:
        #     existing_scanner = Scanner(brand=T3_metadata_json['Manufacturer'],
        #                                model=T3_metadata_json['ManufacturersModelName'],
        #                                source_dataset_id=dataset_object.id, teslas=scanner_teslas)
        #     db.session.add(existing_scanner)
        # db.session.commit()
        #######################################################################
        #######################################################################

        in_csv = (Path(__file__).parent / '../../data/HCA_metadata/ndar_subject01.txt')
        subjects_info_df = pd.read_csv(in_csv, delimiter='\t', skiprows=[1])
        subjects_info_df.set_index('src_subject_id', inplace=True)

        in_csv = (Path(__file__).parent / '../../data/HCA_metadata/vitals01.txt')
        vitals_df = pd.read_csv(in_csv, delimiter='\t', skiprows=[1])
        vitals_df.set_index('src_subject_id', inplace=True)

        in_csv = (Path(__file__).parent / '../../data/HCA_metadata/moca01.txt')
        moca_df = pd.read_csv(in_csv, delimiter='\t', skiprows=[1])
        moca_df.set_index('src_subject_id', inplace=True)


        in_csv = (Path(__file__).parent / '../../data/HCA_metadata/cogcomp01.txt')
        cog_df = pd.read_csv(in_csv, delimiter='\t', skiprows=[1],
                dtype={'nih_fluidcogcomp_ageadjusted': float,
                       'nih_fluidcogcomp_unadjusted': float})
        cog_df.set_index('src_subject_id', inplace=True)


        for i in bucket.objects.filter(Prefix=prefix):
            if (i.key.startswith(prefix)) and (i.size != 0) and ('nii' in i.key.split('.')):
                subject_external_id = i.key.split('/')[-1].split('_')[0]
                image_type = None
                if 'T1w' in i.key:  # and 'MPR1.nii' in i.key:
                    image_type = 'T1'
                elif 'dMRI' in i.key:
                    image_type = 'DWI'
                elif 'rfMRI' in i.key:
                    image_type = 'rsfMRI'
                else:
                    continue
                # Visit parameters:
                visit_external_id = '1' # HCA is not longitudinal
                visit_days_since_baseline = 0
                existing_subject = Subject.query.filter(Subject.external_id == subject_external_id,
                                                        Subject.source_dataset_id == dataset_object.id).first()
                if existing_subject:
                    existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                        Visit.subject_id == existing_subject.id,
                                                        Visit.source_dataset_id == dataset_object.id).first()

                    # Make symptom dict:
                    sym_vital = vitals_df.loc[subject_external_id]
                    sym_moca = moca_df.loc[subject_external_id]
                    try:
                        sym_cog = cog_df.loc[subject_external_id]
                    except KeyError:
                        sym_cog = pd.Series({'nih_fluidcogcomp_ageadjusted': np.nan, 'nih_fluidcogcomp_unadjusted': np.nan})
                    sym_vital['BMI'] = ((sym_vital.weight_std * 0.453592) /
                                        (sym_vital.vtl007 * 0.0254)**2)
                    sym_vital['SSAGA_Educ'] = sym_moca['moca_edu']
                    # sym_cog['nih_fluidcogcomp_unadjusted'] =\
                    #     sym_cog["nih_fluidcogcomp_unadjusted"]
                    # sym_cog['nih_fluidcogcomp_ageadjusted'] =\
                    #     sym_cog["nih_fluidcogcomp_ageadjusted"]
                    sym_sub = subjects_info_df.loc[subject_external_id]
                    sym_series = pd.concat([sym_sub, sym_vital, sym_cog])
                    sym_name_maps = {'BMI': 'bmi',
                                     'SSAGA_Educ': 'education_yrs',
                                     'nih_fluidcogcomp_ageadjusted': 'nih_fluid_cognition_composite_age_adjusted',
                                     'nih_fluidcogcomp_unadjusted': 'nih_fluid_cognition_composite_unadjusted'}
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
                    site_name = subjects_info_df.at[subject_external_id, 'site']
                    ###########################################################
                    # Site / scanner info:
                    ###########################################################
                    if (f'{site_name}_{image_type}') in seen_sites:
                        pass
                    else:
                        source_json = '.'.join(i.key.split('.')[:-2]) + '.json'
                        out_json = (Path(__file__).parent / f'../../data/HCA_metadata/{site_name}_{image_type}.json')
                        with open(out_json, 'wb') as f:
                            bucket.download_fileobj(source_json, f)
                        seen_sites.append(f'{site_name}_{image_type}')
                        site_jsons[f'{site_name}_{image_type}'] = out_json
                        # read json
                    with open(site_jsons[f'{site_name}_{image_type}'], 'r') as t3_json_file:
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

                    ###########################################################
                    # Image info
                    ###########################################################
                    existing_image = Image.query.filter(Image.visit_id == existing_visit.id,
                                                        Image.subject_id == existing_subject.id,
                                                        Image.source_dataset_id == dataset_object.id,
                                                        Image.image_path == i.key).first()
                    if existing_image:
                        existing_image.image_path = i.key
                        existing_image.file_size = i.size
                        existing_image.type = image_type
                        existing_image.days_since_baseline = visit_days_since_baseline
                        existing_image.metadata_json = image_metadata_json
                        existing_image.scanner_id = existing_scanner.id
                        db.session.merge(existing_image)
                    else:
                        image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                         source_dataset_id=dataset_object.id,
                                         image_path=i.key, file_size=i.size,
                                         type=image_type, metadata_json=image_metadata_json,
                                         days_since_baseline=visit_days_since_baseline,
                                         scanner_id=existing_scanner.id)
                        db.session.add(image_db)
                    db.session.commit()
                else:
                    print('ERROR: missing subject id' + subject_external_id)
