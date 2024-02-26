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
from app.models import SourceDataset, Subject, Condition
from load_file.utils import read_s3_contents
from pathlib import Path



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.\
            filter(SourceDataset.designation == 'HCP-Aging').first()

        in_csv = (Path(__file__).parent / '../../data/HCA_metadata/ndar_subject01.txt')
        subjects_info_df = pd.read_csv(in_csv, delimiter='\t', skiprows=[1])
        subjects_info_df.set_index('src_subject_id', inplace=True)

        in_csv = (Path(__file__).parent / '../../data/HCA_metadata/edinburgh_hand01.txt')
        handedness_df = pd.read_csv(in_csv, delimiter='\t', skiprows=[1])
        handedness_df.set_index('src_subject_id', inplace=True)
        for subjid, subject_row in subjects_info_df.iterrows():
            if subject_row['sex'] == 'M':
                gender = 'male'
            elif subject_row['sex'] == 'F':
                gender = 'female'
            temp_handedness = handedness_df.at[subjid, 'hcp_handedness_score']
            if temp_handedness <= 0:
                hand = 'left'
            elif temp_handedness > 0:
                hand = 'right'
            age = subject_row.interview_age / 12
            race_dict = {'Black or African American': 'black',
                         'More than one race': 'multiple',
                         'Asian': 'asian_or_pacific',
                         'American Indian/Alaska Native': 'american_indian',
                         'Unknown or not reported': 'undefined'}
            temp_race = subjects_info_df.at[subjid, 'race']
            temp_ethnicity = subjects_info_df.at[subjid, 'ethnic_group']
            if temp_race in race_dict.keys():
                race = race_dict[temp_race]
            elif (temp_race == 'White') and (temp_ethnicity == 'Not Hispanic or Latino'):
                race = 'white_non_latino'
            elif (temp_race == 'White') and (temp_ethnicity == 'Hispanic or Latino'):
                race = 'white_latino'
            else:
                race = 'undefined'
            existing_subject = Subject.query.filter(Subject.external_id == subjid,
                                                    Subject.source_dataset_id == dataset_object.id).first()
            # At least in HCP NOT AGING all subjects are healthy control
            condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
            if existing_subject:
                existing_subject.external_id = subjid
                existing_subject.gender = gender
                existing_subject.hand = hand
                existing_subject.age_at_baseline = age
                existing_subject.race = race
                existing_subject.condition_id = condition.id
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=subjid, source_dataset_id=dataset_object.id,
                                     gender=gender, hand=hand,
                                     age_at_baseline=age, race=race,
                                     condition_id=condition.id)
                db.session.add(subject_db)
            db.session.commit()
