import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, SourceDataset
from config.globals import ENVIRONMENT
import pdb



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        subjects_info_df = pd.read_csv('../../data/OASIS_metadata/subjects_info.csv')
        clinical_data_df = pd.read_csv('../../data/OASIS_metadata/clinical_data.csv')

        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'OASIS').first()
        for index, subject_row in subjects_info_df.iterrows():
            if subject_row['M/F'] == 'M':
                gender = 'male'
            elif subject_row['M/F'] == 'F':
                gender = 'female'
            if subject_row['Hand'] == 'R':
                hand = 'right'
            elif subject_row['Hand'] == 'L':
                hand = 'left'
            clinical_data_subject = clinical_data_df[clinical_data_df['Subject'] == subject_row.Subject]
            existing_subject = Subject.query.filter(Subject.external_id == subject_row.Subject,Subject.source_dataset_id == dataset_object.id).first()
            if existing_subject:
                existing_subject.external_id = subject_row.Subject
                existing_subject.gender = gender
                existing_subject.hand = hand
                existing_subject.age_at_baseline = clinical_data_subject.iloc[0]['ageAtEntry']
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=subject_row.Subject, source_dataset_id=dataset_object.id, gender=gender
                                     ,hand=hand, age_at_baseline=clinical_data_subject.iloc[0]['ageAtEntry'])
                db.session.add(subject_db)
            db.session.commit()
