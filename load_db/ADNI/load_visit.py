import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, SourceDataset, Visit, Condition
from load_db.ADNI import get_adni_dx
from config.globals import ENVIRONMENT
import pdb
import pickle



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        adni_merge_df = pd.read_csv('../../data/ADNI_metadata/ADNIMERGE.csv')
        adni_merge_df['EXAMDATE'] = pd.to_datetime(adni_merge_df['EXAMDATE'])
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'ADNI').first()
        with open('../../data/ADNI_metadata/ADNISUBJECTIMAGES.pkl', 'rb') as f:
            adnisubjectimages_list = pickle.load(f)
        for index, row in adni_merge_df.iterrows():
            if row['RID'] not in adnisubjectimages_list:
                continue
            existing_subject = Subject.query.filter(Subject.external_id == str(row['RID']),
                                                    Subject.source_dataset_id == dataset_object.id).first()
            if existing_subject:
                visit_external_id = row['VISCODE']
                adni_merge_subject = adni_merge_df[adni_merge_df['RID'] == row['RID']]
                adni_merge_subject_baseline = adni_merge_subject[adni_merge_subject['VISCODE'] == 'bl']
                visit_days_since_baseline = (row['EXAMDATE']-adni_merge_subject_baseline['EXAMDATE'].values[0]).days
                symptoms = dict()
                if pd.notnull(row['MMSE']):
                    symptoms['mmse'] = row['MMSE']
                else:
                    symptoms['mmse'] = None
                if pd.notnull(row['CDRSB']):
                    symptoms['cdr-sb'] = row['CDRSB']
                else:
                    symptoms['cdr-sb'] = None
                existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                    Visit.subject_id == existing_subject.id,
                                                    Visit.source_dataset_id == dataset_object.id).first()
                visit_dx = row['DX']
                condition = get_adni_dx(visit_dx)
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
                print("ERROR: subject id {0} don't exist".format(row['RID']))
