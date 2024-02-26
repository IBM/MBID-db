
import pandas as pd
import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from load_db.AIBL import extract_symptoms, get_aibl_dx
from app import create_app, db
from app.models import Subject, SourceDataset, Visit
from config.globals import ENVIRONMENT

avg_days_per_month = 30.4167

if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        aibl_merged_data = pd.read_csv(Path(__file__).parent / '../../data/AIBL_metadata/aibl_merged_data.csv')
        aibl_merged_data.set_index('Image ID', inplace=True)
        aibl_merged_data['EXAMDATE'] = pd.to_datetime(aibl_merged_data['EXAMDATE'])
        # Removing PET scans from merged data
        aibl_merged_data = aibl_merged_data[aibl_merged_data['Modality'] == 'MRI']
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'AIBL').first()
        for index, row in aibl_merged_data.iterrows():
            existing_subject = Subject.query.filter(Subject.external_id == str(row['RID']),
                                                    Subject.source_dataset_id == dataset_object.id).first()
            if existing_subject:
                visit_external_id = row['VISCODE']
                aibl_subject_specific = aibl_merged_data[aibl_merged_data['RID'] == row['RID']]
                adni_merge_subject_baseline = aibl_subject_specific[aibl_subject_specific['VISCODE'] == 'bl']
                try:
                    visit_days_since_baseline = (row['EXAMDATE']-adni_merge_subject_baseline['EXAMDATE'].values[0]).days
                except:
                    # The current subject is missing baseline visit, so we calculate the days using
                    # the avg days per month and the visit code (m18, m36, m54...)
                    visit_days_since_baseline = int(avg_days_per_month * int(row['VISCODE'].replace("m","")))
                symptoms = extract_symptoms(row)
                existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                    Visit.subject_id == existing_subject.id,
                                                    Visit.source_dataset_id == dataset_object.id).first()
                # Get current diagnosis at this visit
                condition = get_aibl_dx(row['DXCURREN'], symptoms)
                if condition:
                    condition_id = condition.id
                else:
                    condition_id = None
                if existing_visit:
                    existing_visit.days_since_baseline = visit_days_since_baseline
                    existing_visit.symptoms = symptoms
                    existing_visit.condition_id = condition_id
                    db.session.merge(existing_visit)
                else:
                    existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                           source_dataset_id=dataset_object.id,
                                           days_since_baseline=visit_days_since_baseline,
                                           symptoms=symptoms, condition_id=condition_id)
                    db.session.add(existing_visit)
                db.session.commit()
            else:
                print("ERROR: RID {0} don't exist".format(row['RID']))

