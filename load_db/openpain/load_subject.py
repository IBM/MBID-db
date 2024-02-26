import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, SourceDataset
from config.globals import ENVIRONMENT
import pdb
from load_db.openpain import get_openpain_condition



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        subjects_info_df = pd.read_csv('../../data/openpain/participants.tsv', sep='\t')
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'OpenPain').first()
        for index, subject_row in subjects_info_df.iterrows():
            if subject_row['gender'] == 'M':
                gender = 'male'
            elif subject_row['gender'] == 'F':
                gender = 'female'

            condition = get_openpain_condition(subject_row['group'])
            if condition:
                condition_id = condition.id
            else:
                condition_id = None

            condition = subject_row['group']
            existing_subject = Subject.query.filter(Subject.external_id == subject_row.participant_id,
                                                    Subject.source_dataset_id == dataset_object.id).first()
            if existing_subject:
                existing_subject.external_id = subject_row.participant_id
                existing_subject.gender = gender
                existing_subject.age_at_baseline = subject_row.age
                existing_subject.condition_id = condition_id
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=subject_row.participant_id, gender=gender,
                                     age_at_baseline=subject_row.age,
                                     condition_id=condition_id,
                                     source_dataset_id=dataset_object.id)
                db.session.add(subject_db)
            db.session.commit()
