import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, SourceDataset, Visit
from load_db.OASIS import get_oasis_dx
from config.globals import ENVIRONMENT
import pdb



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        clinical_data_df = pd.read_csv('../../data/OASIS_metadata/clinical_data.csv')
        clinical_data_df.loc[:, 'days_since_baseline'] = clinical_data_df.copy()[
                                                                  'ADRC_ADRCCLINICALDATA ID'].str[-4:].astype(int)
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'OASIS').first()
        for index, row in clinical_data_df.iterrows():
            existing_subject = Subject.query.filter(Subject.external_id == row.Subject,
                                                    Subject.source_dataset_id == dataset_object.id).first()
            if existing_subject:
                visit_external_id = row['ADRC_ADRCCLINICALDATA ID']
                visit_days_since_baseline = row['days_since_baseline']
                symptoms = dict()
                if pd.notnull(row['mmse']):
                    symptoms['mmse'] = row['mmse']
                else:
                    symptoms['mmse'] = None
                symptoms['cdr'] = row['cdr']
                bmi = None
                if row['height'] and row['weight']:
                    bmi = (row['height']/row['weight']**2)*703
                existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                    Visit.subject_id == existing_subject.id,
                                                    Visit.source_dataset_id == dataset_object.id).first()
                visit_dx1 = str(row['dx1']).lower()
                visit_cdr = row['cdr']
                condition = get_oasis_dx(visit_cdr, visit_dx1)
                if condition:
                    condition_id = condition.id
                else:
                    condition_id = None
                if existing_visit:
                    existing_visit.days_since_baseline = visit_days_since_baseline
                    existing_visit.symptoms = symptoms
                    existing_visit.condition_id = condition_id
                    existing_visit.bmi = bmi
                    db.session.merge(existing_visit)
                else:
                    existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                           source_dataset_id=dataset_object.id,
                                           days_since_baseline=visit_days_since_baseline,
                                           symptoms=symptoms,
                                           condition_id=condition_id,
                                           bmi=bmi)
                    db.session.add(existing_visit)
                db.session.commit()
            else:
                print("ERROR: subject id {0} don't exist".format(row.subject))
