import psycopg2
import pdb
from sqlalchemy import create_engine
import sqlalchemy.orm as orm
import os
import sys
import pandas as pd
import datetime
import numpy as np
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner
from config.globals import ENVIRONMENT
from load_db.UKBIOBANK import get_file_set, get_file_list, years_education_convertion
from load_file.utils import read_s3_contents
from botocore.client import Config
import ibm_boto3
import types
import os




if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'UK Biobank').first()
        cos_client = ibm_boto3.client('s3',
                                      ibm_api_key_id=app.config['COS_CREDENTIALS_UKBB']['apikey'],
                                      ibm_service_instance_id=app.config['COS_CREDENTIALS_UKBB'][
                                          'resource_instance_id'],
                                      ibm_auth_endpoint=app.config['AUTH_ENDPOINT_UKBB'],
                                      config=Config(signature_version='oauth'),
                                      endpoint_url=app.config['SERVICE_ENDPOINT_UKBB'])
        cos_resource = ibm_boto3.resource('s3',
                                      ibm_api_key_id=app.config['COS_CREDENTIALS_UKBB']['apikey'],
                                      ibm_service_instance_id=app.config['COS_CREDENTIALS_UKBB'][
                                          'resource_instance_id'],
                                      ibm_auth_endpoint=app.config['AUTH_ENDPOINT_UKBB'],
                                      config=Config(signature_version='oauth'),
                                      endpoint_url=app.config['SERVICE_ENDPOINT_UKBB'])
        bucket_name = app.config['BUCKET_UKBB']
        missing_subjects = {}
        postgresql_url = app.config['SQLALCHEMY_DATABASE_UKBB_URI']
        engine = create_engine(postgresql_url)
        SessionSiteDB = orm.sessionmaker(bind=engine)
        sessionSiteDB = SessionSiteDB()
        query = """SELECT eid, field_id, instance_index, value
        FROM structured_data.ukbb_data_202106
        where (field_id=53 and (instance_index = 0 or instance_index = 2 or instance_index = 3))
        OR (field_id=6373 and (instance_index = 2 or instance_index = 3))
        OR (field_id=6333 and (instance_index = 2 or instance_index = 3))
        OR (field_id=21001 and (instance_index = 2 or instance_index = 3))
        OR (field_id=845 and (instance_index = 0))
        OR (field_id=6138 and (instance_index = 0))"""
        image_dates_df = pd.read_sql(query, sessionSiteDB.bind)
        # For now we only process t1 images
        #for modality in [20227,20250, 20252]:
        for modality in [20252]:
            missing_subjects[modality] = dict()
            existing_file_set = get_file_list(cos_client=cos_client, bucket_name=bucket_name,contains_key=str(modality), starts_with='unzipped')
            for file in existing_file_set:
                if not file['Key'].endswith('T1.json'):
                    subject_external_id = file['Key'].replace('unzipped/', '').split('_')[0]
                    instance_index = file['Key'].replace('unzipped/', '').split('_')[2]
                    existing_subject = Subject.query.filter(Subject.external_id == subject_external_id,
                                                            Subject.source_dataset_id == dataset_object.id).first()
                    if existing_subject:
                        print('Subject exists')
                        subject_instance = image_dates_df[(image_dates_df['field_id']==53) & (image_dates_df['eid']==int(subject_external_id)) & (image_dates_df['instance_index']==int(instance_index))]
                        if subject_instance.empty:
                            print('Missing date for image')
                        else:
                            initial_subject_instance = image_dates_df[(image_dates_df['field_id']==53) & (image_dates_df['eid']==int(subject_external_id)) & (image_dates_df['instance_index']==0)]
                            if initial_subject_instance.empty:
                                print('Missing date for initial assessment')
                            else:
                                initial_subject_date = datetime.datetime.strptime(initial_subject_instance['value'].values[0], '%Y-%m-%d').date()
                                image_subject_date = datetime.datetime.strptime(subject_instance['value'].values[0],
                                                                                  '%Y-%m-%d').date()
                                days_since_baseline = (image_subject_date-initial_subject_date).days
                                type = None
                                if modality == 20250:
                                    type = 'DWI'
                                elif modality == 20227:
                                    type = 'rsfMRI'
                                elif modality == 20252:
                                    type = 'T1'
                                if type == 'T1':
                                    image_metadata = "/".join(file['Key'].split('/')[:-1])+'/T1.json'

                                try:
                                    image_metadata_content = read_s3_contents(cos_resource, app.config['BUCKET_UKBB'],
                                                                              image_metadata)
                                except cos_resource.meta.client.exceptions.NoSuchKey:
                                    print("Can't find image metadata for file " + image_metadata + ". Will use generic image metadata.")
                                    # T1 metadata files that we checked were all the same, will use a generic one when missing
                                    image_metadata = 'T1.json'
                                    image_metadata_content = read_s3_contents(cos_resource, app.config['BUCKET_UKBB'],
                                                                              image_metadata)
                                image_metadata_json = json.loads(image_metadata_content.decode("utf-8"))
                                fluid_intl_matrix_n_correct = image_dates_df[(image_dates_df['field_id']==6373) & (image_dates_df['eid']==int(subject_external_id)) & (image_dates_df['instance_index']==int(instance_index))]['value'].values
                                fluid_intl_matrix_RT = image_dates_df[(image_dates_df['field_id']==6333) & (image_dates_df['eid']==int(subject_external_id)) & (image_dates_df['instance_index']==int(instance_index))]['value'].values
                                bmi = image_dates_df[(image_dates_df['field_id']==21001) & (image_dates_df['eid']==int(subject_external_id)) & (image_dates_df['instance_index']==int(instance_index))]['value'].values
                                field845 = image_dates_df[(image_dates_df['field_id'] == 845) & (
                                            image_dates_df['eid'] == int(subject_external_id))]['value'].values
                                field6138 = image_dates_df[(image_dates_df['field_id'] == 6138) & (image_dates_df['eid'] == int(subject_external_id))]['value'].values
                                if fluid_intl_matrix_n_correct.any():
                                    fluid_intl_matrix_n_correct_value = fluid_intl_matrix_n_correct[0]
                                else:
                                    fluid_intl_matrix_n_correct_value = None
                                if fluid_intl_matrix_RT.any():
                                    fluid_intl_matrix_RT_value = fluid_intl_matrix_RT[0]
                                else:
                                    fluid_intl_matrix_RT_value = None
                                if bmi.any():
                                    bmi_value = bmi[0]
                                else:
                                    bmi_value = None
                                if field845.any():
                                    field845_value = field845[0]
                                else:
                                    field845_value = None
                                if field6138.any():
                                    field6138_value = field6138[0]
                                else:
                                    field6138_value = None

                                education_yrs = years_education_convertion(field845_value, field6138_value)
                                symptoms = {'bmi': bmi_value,
                                'education_yrs': education_yrs,
                                'fluid_intl_matrix_n_correct': fluid_intl_matrix_n_correct_value,
                                'fluid_intl_matrix_RT': fluid_intl_matrix_RT_value}
                                if 'DeviceSerialNumber' in image_metadata_json:
                                    serial_number = image_metadata_json['DeviceSerialNumber']
                                else:
                                    serial_number = None
                                existing_scanner = Scanner.query.filter(
                                    Scanner.brand == image_metadata_json['Manufacturer'],
                                    Scanner.model == image_metadata_json['ManufacturersModelName'],
                                    Scanner.source_id == serial_number,
                                    Scanner.source_dataset_id == dataset_object.id).first()

                                scanner_teslas = None
                                if image_metadata_json['MagneticFieldStrength'] == '3' or image_metadata_json[
                                    'MagneticFieldStrength'] == 3:
                                    scanner_teslas = 'three'
                                elif image_metadata_json['MagneticFieldStrength'] == '1.5' or image_metadata_json[
                                    'MagneticFieldStrength'] == 1.5:
                                    scanner_teslas = 'one_and_a_half'
                                if existing_scanner:
                                    existing_scanner.teslas = scanner_teslas
                                    db.session.merge(existing_scanner)
                                else:
                                    existing_scanner = Scanner(brand=image_metadata_json['Manufacturer'],
                                                               model=image_metadata_json['ManufacturersModelName'],
                                                               source_id=serial_number,
                                                               source_dataset_id=dataset_object.id,
                                                               teslas=scanner_teslas)
                                    db.session.add(existing_scanner)
                                db.session.commit()

                                existing_visit = Visit.query.filter(Visit.external_id == instance_index,
                                                                    Visit.subject_id == existing_subject.id,
                                                                    Visit.source_dataset_id == dataset_object.id).first()
                                if existing_visit:
                                    existing_visit.days_since_baseline = days_since_baseline
                                    existing_visit.symptoms = symptoms
                                    db.session.merge(existing_visit)
                                else:
                                    existing_visit = Visit(external_id=instance_index, subject_id=existing_subject.id,
                                                           source_dataset_id=dataset_object.id,
                                                           days_since_baseline=days_since_baseline,
                                                           symptoms=symptoms)
                                    db.session.add(existing_visit)
                                db.session.commit()

                                existing_image = Image.query.filter(Image.visit_id == existing_visit.id,
                                                                    Image.subject_id == existing_subject.id,
                                                                    Image.source_dataset_id == dataset_object.id,
                                                                    Image.image_path == file['Key']).first()
                                if existing_image:
                                    existing_image.image_path = file['Key']
                                    existing_image.file_size = file['Size']
                                    existing_image.type = type
                                    existing_image.days_since_baseline = days_since_baseline
                                    existing_image.metadata_json = image_metadata_json
                                    existing_image.scanner_id = existing_scanner.id
                                    db.session.merge(existing_image)
                                else:
                                    image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                                     source_dataset_id=dataset_object.id,
                                                     image_path=file['Key'], file_size=file['Size'],
                                                     type=type, metadata_json=image_metadata_json,
                                                     days_since_baseline=days_since_baseline,
                                                     scanner_id=existing_scanner.id)
                                    db.session.add(image_db)
                                db.session.commit()
                    else:
                        print("Image of subject id {0}, which doesn't exist in DB".format(subject_external_id))

