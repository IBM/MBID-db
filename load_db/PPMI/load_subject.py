import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import logging
from app import create_app, db
from load_db.PPMI import get_ppmi_condition
from app.models import Subject, SourceDataset
from config.globals import ENVIRONMENT


if __name__ == '__main__':
    
    # Create logger
    logger = logging.getLogger(__name__)  
    logger.setLevel(logging.INFO)

    # create file and console loggers with different log levels
    fh = logging.FileHandler('debug_subject.log')
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
        # Load baseline info of subjects (generated in explore_ppmi.py)
        PPMI_study_name = "Parkinson's Progression Markers Initiative"
        subjects_info_df = pd.read_csv('../../data/PPMI_metadata/'
                                       'demographics_info.csv',
                                       dtype={'PATNO': str, 'educ_yrs': int})
        dataset_object = SourceDataset.query.filter(SourceDataset.designation
                                                    == PPMI_study_name).first()

        # Iterate through entries of subject dataframe; populate database
        for index, subject_row in subjects_info_df.iterrows():
            print('Subject #{0} of {1}'.format(index+1,
                                               subjects_info_df.shape[0]))
            existing_subject =\
                Subject.query.filter(Subject.external_id == subject_row.PATNO,
                                     Subject.source_dataset_id ==
                                     dataset_object.id).first()

            # Retrieve condition representation in DB
            condition = get_ppmi_condition(subject_row['condition'])
            if condition:
                condition_id = condition.id
            else:
                condition_id = None

            if existing_subject:
                existing_subject.external_id = subject_row.PATNO
                existing_subject.gender = subject_row.gender
                existing_subject.race = subject_row.race
                existing_subject.condition_id = condition_id
                existing_subject.education_yrs = subject_row.educ_yrs
                existing_subject.age_at_baseline = subject_row.age
                db.session.merge(existing_subject)
                logger.info('Updating Subject {} info'\
                            .format(subject_row.PATNO))
            else:
                subject_db = Subject(external_id=subject_row.PATNO,
                                     gender=subject_row.gender,
                                     race=subject_row.race,
                                     condition_id=condition_id,
                                     education_yrs=subject_row.educ_yrs,
                                     age_at_baseline=subject_row.age,
                                     source_dataset_id=dataset_object.id)
                logger.info('Creating Subject {} info'\
                            .format(subject_row.PATNO))
                db.session.add(subject_db)
            db.session.commit()
