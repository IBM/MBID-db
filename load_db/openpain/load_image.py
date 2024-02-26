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
import pdb
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner
from load_file.utils import read_s3_contents
from load_db.openpain import get_openpain_condition


if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'OpenPain').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'openpain.org'
        T1_metadata = 'openpain.org/subacute_longitudinal_study/T1w.json'
        T1_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'], T1_metadata)
        T1_metadata_json = json.loads(T1_metadata_content.decode("utf-8"))
        dwi_metadata = 'openpain.org/subacute_longitudinal_study/dwi.json'
        dwi_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'], dwi_metadata)
        dwi_metadata_json = json.loads(dwi_metadata_content.decode("utf-8"))
        task_resting_bold_metadata = 'openpain.org/subacute_longitudinal_study/task-resting_bold.json'
        task_resting_bold_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'], task_resting_bold_metadata)
        task_resting_bold_metadata_json = json.loads(task_resting_bold_metadata_content.decode("utf-8"))
        existing_scanner = Scanner.query.filter(Scanner.brand == T1_metadata_json['Manufacturer'],
                                            Scanner.model == T1_metadata_json['ManufacturersModelName'],
                                            Scanner.source_dataset_id == dataset_object.id).first()
        scanner_teslas = None
        if T1_metadata_json['MagneticFieldStrength'] == '3':
            scanner_teslas = 'three'
        elif T1_metadata_json['MagneticFieldStrength'] == '1.5':
            scanner_teslas = 'one_and_a_half'
        if existing_scanner:
            existing_scanner.teslas = scanner_teslas
            db.session.merge(existing_scanner)
        else:
            existing_scanner = Scanner(brand=T1_metadata_json['Manufacturer'], model=T1_metadata_json['ManufacturersModelName'],
                                   source_dataset_id=dataset_object.id, teslas=scanner_teslas)
            db.session.add(existing_scanner)
        db.session.commit()
        for i in bucket.objects.all():
            if (i.key.startswith(prefix)) and (i.size != 0) and ('nii' in i.key.split('.')):
                subject_external_id = 'sub-'+i.key.split('sub-')[1].split('/')[0]
                sessions_metadata = i.key.split('sub-')[0] + subject_external_id +'/'+subject_external_id +'_sessions.tsv'
                sessions_metadata_content = read_s3_contents(cos_client,app.config['BUCKET'],sessions_metadata)
                sessions_metadata_df = pd.read_csv(StringIO(sessions_metadata_content.decode("utf-8") ), sep='\t')

                session_metadata_df = sessions_metadata_df[
                    sessions_metadata_df['session_id'] == 'visit' + i.key.split('ses-visit')[1].split('/')[0]]
                symptoms = dict()
                if pd.notnull(session_metadata_df['mpq_vas'].values[0]):
                    symptoms['mpq_vas'] = session_metadata_df['mpq_vas'].values[0]
                else:
                    symptoms['mpq_vas'] = None
                if isinstance(session_metadata_df['bdi_total'].values[0],str):
                    symptoms['bdi_total'] = None
                else:
                    if pd.notnull(session_metadata_df['bdi_total'].values[0]):
                        symptoms['bdi_total'] = float(session_metadata_df['bdi_total'].values[0])
                    else:
                        symptoms['bdi_total'] = None
                        
                subjects_info_df =\
                    pd.read_csv('../../data/openpain/participants.tsv', sep='\t')
                condition =\
                    subjects_info_df.loc[subjects_info_df.participant_id
                                         == subject_external_id, 'group']
                condition = get_openpain_condition(condition.squeeze())
                if condition:
                    condition_id = condition.id
                else:
                    condition_id = None

                visit_external_id = 'ses-visit' + i.key.split('ses-visit')[1].split('/')[0]
                if pd.notnull(session_metadata_df[
                    'days_since_interview'].values[0]):
                    days_since_baseline = int(session_metadata_df[
                        'days_since_interview'].values[0])
                else:
                    days_since_baseline = None
                type = None
                if 'func' in i.key and 'task' in i.key:
                    continue
                elif 'anat' in i.key and 'T2' in i.key:
                    continue
                elif 'func' in i.key and 'resting_bold' in i.key:
                    type = 'rsfMRI'
                    image_metadata_json = task_resting_bold_metadata_json
                elif 'anat' in i.key and 'T1' in i.key:
                    type = 'T1'
                    image_metadata_json = T1_metadata_json
                elif 'dwi' in i.key:
                    type = 'DWI'
                    image_metadata_json = dwi_metadata_json

                existing_subject = Subject.query.filter(Subject.external_id == subject_external_id,
                                                    Subject.source_dataset_id == dataset_object.id).first()
                if existing_subject:
                    existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                        Visit.subject_id == existing_subject.id,
                                                            Visit.source_dataset_id == dataset_object.id).first()
                    if existing_visit:
                        existing_visit.days_since_baseline = days_since_baseline
                        existing_visit.symptoms = symptoms
                        existing_visit.condition_id = condition_id
                        db.session.merge(existing_visit)
                    else:
                        existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                               source_dataset_id=dataset_object.id, days_since_baseline=days_since_baseline,
                                               condition_id=condition_id,
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
                        existing_image.metadata_json = image_metadata_json
                        existing_image.scanner_id = existing_scanner.id
                        db.session.merge(existing_image)
                    else:
                        image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                             source_dataset_id=dataset_object.id,
                                             image_path=i.key, file_size=i.size,
                                         type=type, metadata_json=image_metadata_json,
                                         scanner_id=existing_scanner.id)
                        db.session.add(image_db)
                    db.session.commit()
                else:
                    print('ERROR: missing subject id '+subject_external_id)
