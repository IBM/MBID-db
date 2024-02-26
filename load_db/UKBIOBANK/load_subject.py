from botocore.client import Config
import ibm_boto3
from io import StringIO
import types
import os
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy.orm as orm
import sys
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import pdb
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner
from load_db.UKBIOBANK import years_education_convertion
from load_file.utils import read_s3_contents
from pathlib import Path



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'UK Biobank').first()
        postgresql_url = app.config['SQLALCHEMY_DATABASE_UKBB_URI']
        engine = create_engine(postgresql_url)
        SessionSiteDB = orm.sessionmaker(bind=engine)
        sessionSiteDB = SessionSiteDB()
        # If subject don't have a t1 image in the db, we ignore it
        subjects_db = sessionSiteDB.execute("""SELECT * FROM(
                        select 
                        eid, 
                        MAX(CASE WHEN field_id = 21022 THEN value end) as age_value,
                        MAX(CASE WHEN field_id = 31 THEN value end) as sex_value,
                        MAX(CASE WHEN field_id = 1707 THEN value end) as handedness_value,
                        MAX(CASE WHEN field_id = 845 THEN value end) as age_finish_education,
                        MAX(CASE WHEN field_id = 6138 THEN value end) as max_education_obtained,
                        MAX(CASE WHEN field_id = 21000 THEN value end) as race_value,
                        MAX(CASE WHEN field_id = 20252 THEN value end) as t1_image_value
                        FROM structured_data.ukbb_data_202106
                        where field_id=21022 or field_id=31 or field_id=1707 or field_id=845 or field_id=21000
                        or field_id=20252 or field_id=6138
                        group by eid
                        order by eid) as demographics_query
                        where t1_image_value is not null 
                        """)
        columns_names = subjects_db.keys()._keys

        race_dict = {
            # White
            '1':'white_non_latino',
            # British
            '1001': 'white_non_latino',
            # White and Black Caribbean
            '2001': 'undefined',
            # Indian
            '3001': 'asian_or_pacific',
            # Caribbean
            '4001': 'undefined',
            # Irish
            '1002': 'white_non_latino',
            # White and Black African
            '2002': 'undefined',
            # Pakistani
            '3002': 'asian_or_pacific',
            # African
            '4002': 'black',
            # Any other white background
            '1003': 'white_non_latino',
            # White and Asian
            '2003': 'white_non_latino',
            # Bangladeshi
            '3003': 'asian_or_pacific',
            # Any other Black background
            '4003': 'black',
            # Any other mixed background
            '2004': 'undefined',
            # Any other Asian background
            '3004': 'asian_or_pacific',
                     # Black or Black British
                     '4': 'black',
                      #Mixed
                     '2': 'multiple',
                      #Asian or Asian British
                     '3': 'asian_or_pacific',
                      #Chinese
                     '5': 'asian_or_pacific',
                      #Other ethnic group
                     '6': 'undefined',
                      # Do not know
                     '-1': 'undefined',
                      # Prefer not to answer
                     '-3': 'undefined'}
        for subject in subjects_db:
            # Data-Coding 9, male 1, female 0
            if subject[columns_names.index('sex_value')] == '1':
                gender = 'male'
            elif subject[columns_names.index('sex_value')] == '0':
                gender = 'female'
            # Data-Coding 100430
            if subject[columns_names.index('handedness_value')] == '2' :
                hand = 'left'
            elif subject[columns_names.index('handedness_value')] == '1' :
                hand = 'right'
            elif subject[columns_names.index('handedness_value')] == '3':
                hand = 'ambidextrous'
            elif subject[columns_names.index('handedness_value')] == '-3':
                hand = 'undefined'
            # Data-Coding 1001
            if subject[columns_names.index('race_value')]:
                race = race_dict[subject[columns_names.index('race_value')]]
            else:
                race = None
            subjid = str(subject[0])
            existing_subject = Subject.query.filter(Subject.external_id == subjid,
                                                    Subject.source_dataset_id == dataset_object.id).first()
            education_years = years_education_convertion(subject[columns_names.index('age_finish_education')],subject[columns_names.index('max_education_obtained')])
            if existing_subject:
                existing_subject.external_id = subjid
                existing_subject.gender = gender
                existing_subject.hand = hand
                existing_subject.age_at_baseline = subject[columns_names.index('age_value')]
                existing_subject.education_yrs=education_years,
                existing_subject.race = race
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=subjid, source_dataset_id=dataset_object.id,
                                     gender=gender, hand=hand,
                                     age_at_baseline=subject[columns_names.index('age_value')],
                                     education_yrs=education_years,
                                     race=race)
                db.session.add(subject_db)
            db.session.commit()

