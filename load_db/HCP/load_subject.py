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
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'Human connectome project').first()

        subjects_info_df = pd.read_csv(Path(__file__).parent / '../../data/HCP1200_metadata/Behavioral_Individual_Subject_Measures.csv',
                                       dtype={'Subject': str})
        subjects_info_df.set_index('Subject', inplace=True)
        restricted_df = pd.read_csv(Path(__file__).parent / '../../data/HCP1200_metadata/RESTRICTED_ecastrow_8_12_2021_16_51_2.csv',
                                    dtype={'Subject': str})
        restricted_df.set_index('Subject', inplace=True)
        for subjid, subject_row in subjects_info_df.iterrows():
            if subject_row['Gender'] == 'M':
                gender = 'male'
            elif subject_row['Gender'] == 'F':
                gender = 'female'
            if restricted_df.at[subjid, 'Handedness'] <= 0 :
                hand = 'left'
            elif restricted_df.at[subjid, 'Handedness'] > 0 :
                hand = 'right'
            age = int(restricted_df.at[subjid, 'Age_in_Yrs'])
            race_dict = {'Black or African Am.': 'black',
                         'More than one': 'multiple',
                         'Asian/Nat. Hawaiian/Othr Pacific Is.': 'asian_or_pacific',
                         'Am. Indian/Alaskan Nat.': 'american_indian',
                         'Unknown or Not Reported': 'undefined'}
            temp_race = restricted_df.at[subjid, 'Race']
            temp_ethnicity = restricted_df.at[subjid, 'Ethnicity']
            if temp_race in race_dict.keys():
                race = race_dict[temp_race]
            elif (temp_race == 'White') and (temp_ethnicity == 'Not Hispanic/Latino'):
                race = 'white_non_latino'
            elif (temp_race == 'White') and (temp_ethnicity == 'Hispanic/Latino'):
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
