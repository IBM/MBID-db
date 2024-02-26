import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import SourceDataset, Subject, Condition
from pathlib import Path



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.\
            filter(SourceDataset.designation == 'SALD').first()

        in_csv = (Path(__file__).parent / '../../data/SALD_metadata/demographic_information.csv')
        subjects_info_df = pd.read_csv(in_csv, delimiter=',')
        subjects_info_df.set_index('Sub_ID', inplace=True)

        for subjid, subject_row in subjects_info_df.iterrows():
            str_subjid = str(subjid)
            if subject_row['Sex'] in ['M', 'M ']:
                gender = 'male'
            elif subject_row['Sex'] in ['F', 'F ']:
                gender = 'female'
            else:
                print("No gender was specified")
            temp_handedness = subject_row["Edinburgh Handedness Inventory (EHI)"]
            if temp_handedness <= 0:
                hand = 'left'
            elif temp_handedness > 0:
                hand = 'right'
            else:
                hand = None
            age = subject_row["Age"]
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
