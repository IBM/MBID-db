import os
import pandas as pd
import sys
from botocore.client import Config
import ibm_boto3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from load_db.NKIRS import get_baseline_visit, open_subject_metadata_file
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import SourceDataset, Subject, Condition
from pathlib import Path



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.\
            filter(SourceDataset.designation == 'NKIRS').first()

        in_csv = (Path(__file__).parent / '../../data/NKIRS_metadata/participants.tsv')
        participants_info = pd.read_csv(in_csv, delimiter=',')
        participants_info.set_index('participant_id', inplace=True)

        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'NKIRS'
        avoid = ['sub-A00028995', 'sub-A00051548', 'sub-A00037229', 'sub-A00039277', 'sub-A00056099', 'sub-A00056295',
        	'sub-A00057726', 'sub-A00061413', 'sub-A00066248', 'sub-A00074447', 'sub-A00075292', 'sub-A00082665', 'sub-A00085866']
        for subjid, subject_row in participants_info.iterrows():
            str_subjid = str(subjid)
            if str_subjid in avoid:
                continue
            if subject_row['sex'] in ['M', 'M ']:
                gender = 'male'
            elif subject_row['sex'] in ['F', 'F ']:
                gender = 'female'
            else:
                print("No gender was specified")
                continue
            temp_handedness = subject_row["handedness"]
            if temp_handedness == 'L':
                hand = 'left'
            elif temp_handedness == 'R':
                hand = 'right'
            else:
                hand = None
            
            subject_info_df = open_subject_metadata_file(app, cos_client, subjid)
            baseline_visit = get_baseline_visit(subject_info_df)
            try:
                age = subject_info_df.loc[baseline_visit]['age'].values[0]
            except AttributeError:
                try:
                   age = subject_info_df.loc[baseline_visit]['age']
                except:
                    print(f'No available age at baseline: {subject_info_df}')
                    continue
            except KeyError:
                print(f"No baseline visit {subject_info_df}")
                continue
            existing_subject = Subject.query.filter(Subject.external_id == str_subjid,
                                                    Subject.source_dataset_id == dataset_object.id).first()
            # In SALD all subjects are healthy control
            condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
            if existing_subject:
                existing_subject.external_id = str_subjid
                existing_subject.gender = gender
                existing_subject.hand = hand
                existing_subject.age_at_baseline = age
                existing_subject.condition_id = condition.id
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=str_subjid, source_dataset_id=dataset_object.id,
                                     gender=gender, hand=hand,
                                     age_at_baseline=age,
                                     condition_id=condition.id)
                db.session.add(subject_db)
            db.session.commit()
