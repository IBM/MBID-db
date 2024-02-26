import os
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import SourceDataset, Subject, Condition
from pathlib import Path
from load_db.CamCAN import years_education_convertion



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.\
            filter(SourceDataset.designation == 'Cam-CAN').first()

        in_csv = (Path(__file__).parent / '../../data/Cam-CAN/standard_data.csv')
        standard_data_df = pd.read_csv(in_csv, delimiter=',')
        standard_data_df.set_index('CCID', inplace=True)

        in_csv = (Path(__file__).parent / '../../data/Cam-CAN/approved_data.tsv')
        approved_data_df = pd.read_csv(in_csv, delimiter='\t')
        approved_data_df.set_index('CCID', inplace=True)
        
        # Pre-processing metadata to get years of study
        columns = ['cse_years', 'nvq1_bteci_years', 'o_level_gcse_leaving_years', 'nvq2_btec1_years', 'a_level_ib_years', 'nvq3_btecd_years',
        'hnc_hnd_nvq4_btecp_years', 'btec_ad_years', 'col_dip_years', 'undergrad_years', 'masters_years',
        'phd_doc_years']
        final_csv = None
        for column in columns:
            # Get the studies from all stage in life (YA young adulthood, ML Midlife, LL Laterlife)
            # Add them alltogether into one column
            other_csv = approved_data_df[approved_data_df.columns[approved_data_df.columns.to_series().str.contains(column)]]\
                .sum(axis=1).to_frame(name=column)
            if final_csv is None:
                final_csv = other_csv
            else: 
                # Merge the current data into the aggregate data
                final_csv = final_csv.merge(other_csv,on='CCID', how='inner')
        for subjid, subject_row in standard_data_df.iterrows():
            str_subjid = str(subjid)
            if subject_row['Sex'] in ['MALE', 'male']:
                gender = 'male'
            elif subject_row['Sex'] in ['FEMALE', 'female']:
                gender = 'female'
            else:
                print("No gender was specified")
            temp_handedness = subject_row["Hand"]
            if temp_handedness <= 0:
                hand = 'left'
            elif temp_handedness > 0:
                hand = 'right'
            else:
                print("No hand specified")
                hand = None
            age = subject_row["Age"]
            existing_subject = Subject.query.filter(Subject.external_id == str_subjid,
                                                    Subject.source_dataset_id == dataset_object.id).first()

            # Handle the years of study of the subjects
            education_yrs = years_education_convertion(final_csv.loc[str_subjid])
            # 28 is a threshold to remove incorrect data while filling the form
            if education_yrs is None or education_yrs >= 28.0:
                education_yrs = None
            # In Cam-CAN all subjects are healthy control
            condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
            if existing_subject:
                existing_subject.external_id = str_subjid
                existing_subject.gender = gender
                existing_subject.hand = hand
                existing_subject.age_at_baseline = age
                existing_subject.condition_id = condition.id
                existing_subject.education_yrs = education_yrs
                db.session.merge(existing_subject)
            else:
                subject_db = Subject(external_id=str_subjid, source_dataset_id=dataset_object.id,
                                     gender=gender, hand=hand,
                                     age_at_baseline=age,
                                     condition_id=condition.id,
                                     education_yrs= education_yrs)
                db.session.add(subject_db)
            db.session.commit()
