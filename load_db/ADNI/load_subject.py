import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from load_db.ADNI import get_adni_dx, get_visit_code
from app import create_app, db
from app.models import Subject, SourceDataset
from config.globals import ENVIRONMENT

def race(ethnicity, race):
    if race == 5: # White
        if ethnicity == 1: # Hisp/Latino
            race_value = 'white_latino'
        else:
            race_value = 'white_non_latino'
    elif race == 1: # Am Indian/Alaskan
        race_value = 'american_indian'
    elif race == 6: # More than one
        race_value = 'multiple'
    elif race == 2 or race == 3: # Asian or Native Hawaiian or Other Pacific Islander
        race_value = 'asian_or_pacific'
    elif race == 4: # Black
        race_value = 'black'
    else:
        race_value = 'undefined'
    return race_value

if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        adni_images_merge_t1 = pd.read_csv('./data/ADNI_all_images/metadata/T1_merge_nodup.csv')
        adni_images_merge_t1.set_index('Subject ID', inplace=True)
        adni_images_merge_t1['Study Date'] = pd.to_datetime(adni_images_merge_t1['Study Date'], infer_datetime_format=True) #format='%m/%d/%Y')
        adni_images_merge_t1 = adni_images_merge_t1.sort_values(by='Study Date', ascending=True)
        adni_images_merge_t1 = adni_images_merge_t1[adni_images_merge_t1.apply(lambda x: get_visit_code(x)=='bl',axis=1)]
        adni_demographics_df = pd.read_csv('./data/ADNI_all_images/metadata/ADNI_tabular/PTDEMOG.csv')
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'ADNI').first()
        already_added = set()
        no_data = {
            'gender': set(),
            'hand': set(),
            'condition': set(),
            'race': set(),
            'education_yrs': set(),
        }
        for index, subject_row in adni_images_merge_t1.iterrows():
            rid = int(str(index).split('_')[-1])
            if rid in already_added:
                continue
            adni_demographics_row = adni_demographics_df[adni_demographics_df['RID']==rid].iloc[0]
            if subject_row['Sex'] == 'M':
                gender = 'male'
            elif subject_row['Sex'] == 'F':
                gender = 'female'
            else:
                no_data['gender'].add(rid)
                print(f'Error on SEX: {rid}, {subject_row["Sex"]}')
            if adni_demographics_row['PTHAND'] == 1:
                hand = 'right'
            elif adni_demographics_row['PTHAND'] == 2:
                hand = 'left'
            else:
                print(f'Error on HAND: {rid}, {adni_demographics_row["PTHAND"]}')
                no_data['hand'].add(rid)
            if adni_demographics_row['PTEDUCAT'] is None or adni_demographics_row['PTEDUCAT'] < 0:
                no_data['education_yrs'].add(rid)
                print(f'Error on Education years:{rid}')
            existing_subject = Subject.query.filter(Subject.external_id == str(rid),Subject.source_dataset_id == dataset_object.id).first()
            race_value = race(adni_demographics_row['PTETHCAT'], adni_demographics_row['PTRACCAT'])
            if race_value == 'undefined':
                no_data['race'].add(rid)
            condition = get_adni_dx(subject_row['Research Group'])
            if condition:
                condition_id = condition.id
            else:
                condition_id = None
                no_data['condition'].add(rid)
                print(f'ERROR on condition: RID {rid}')
            if existing_subject:
                existing_subject.external_id = str(rid)
                existing_subject.gender = gender
                existing_subject.hand = hand
                existing_subject.condition_id = condition_id
                existing_subject.age_at_baseline = subject_row['Age']
                existing_subject.education_yrs = adni_demographics_row['PTEDUCAT']
                existing_subject.race = race_value
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=str(rid), source_dataset_id=dataset_object.id, gender=gender
                                     ,hand=hand, age_at_baseline=subject_row['Age'],education_yrs=adni_demographics_row['PTEDUCAT'],
                                     race=race_value, condition_id=condition_id)
                db.session.add(subject_db)
            already_added.add(rid)
            db.session.commit()
        print('\nBalance:')
        for i in no_data:
            print(f'    {i}:{len(no_data[i])}')
        print(f'Subject count: {len(already_added)}')