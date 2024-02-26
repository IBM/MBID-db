import pandas as pd
import os
import sys
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, SourceDataset
from config.globals import ENVIRONMENT

avg_days_per_month = 30.4167
if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        aibl_subject_data_complete = pd.read_csv(Path(__file__).parent / '../../data/AIBL_metadata/aibl_merged_data.csv')
        aibl_subject_data_complete.set_index('Image ID', inplace=True)
        aibl_subject_data_complete = aibl_subject_data_complete.drop_duplicates(subset=['RID', 'VISCODE'])
        aibl_subject_data = aibl_subject_data_complete[aibl_subject_data_complete['VISCODE']=='bl']
        # Some subjects are missing baseline visit, so using the m18 to get the data
        for subject in [200, 263, 276, 378]:
            subject_row = aibl_subject_data_complete[(aibl_subject_data_complete["RID"] == subject) & (aibl_subject_data_complete['VISCODE']=='m18')]
            index = subject_row.index.values.astype(int)[0]
            aibl_subject_data = pd.concat([aibl_subject_data,subject_row])
            aibl_subject_data.at[index, "Age"] = subject_row["Age"].values[0] - 1.5
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'AIBL').first()
        for index, subject_row in aibl_subject_data.iterrows():
            if subject_row['Sex'] == 'M':
                gender = 'male'
            elif subject_row['Sex'] == 'F':
                gender = 'female'
            else:
                gender = 'undefined'
            existing_subject = Subject.query.filter(Subject.external_id == str(subject_row['RID']),Subject.source_dataset_id == dataset_object.id).first()
            if existing_subject:
                existing_subject.external_id = str(subject_row['RID'])
                existing_subject.gender = gender
                existing_subject.age_at_baseline = subject_row['Age']
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=str(subject_row['RID']), 
                                        source_dataset_id=dataset_object.id, 
                                        gender=gender,
                                        age_at_baseline=subject_row['Age'])
                db.session.add(subject_db)
            db.session.commit()
