import pandas as pd
import os
import sys
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app
from config.globals import ENVIRONMENT


visit_dictionary = {
    "Baseline": "bl",
    "18 Month follow-up": "m18",
    "36 Month follow-up": "m36",
    "54 Month follow-up": "m54",
    "72 Month follow-up": "m72"
}
if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        files = ['aibl_mmse_01-Jun-2018.csv', 'aibl_cdr_01-Jun-2018.csv', 'aibl_labdata_01-Jun-2018.csv', 
            'aibl_bslcheck_01-Jun-2018.csv', 'aibl_neurobat_01-Jun-2018.csv', 'aibl_pdxconv_01-Jun-2018.csv']
        aibl_subject_data = pd.read_csv(Path(__file__).parent / '../../data/AIBL_metadata/idaSearch_9_02_2022 complete.csv')
        for i, file in enumerate(files):
            new_data = pd.read_csv(Path(__file__).parent / f'../../data/AIBL_metadata/Data_extract_3.3.0/{file}')
            new_data.set_index(['RID','VISCODE'] , inplace=True)
            if i == 0:
                aibl_merge_data = new_data
            else:
                suffix = file.replace('_01-Jun-2018.csv', '').replace('aibl', '')
                aibl_merge_data = aibl_merge_data.join(new_data, on=['RID', 'VISCODE'], how='left', rsuffix=suffix)
        aibl_subject_data.rename(columns={'Subject ID':'RID', 'Visit':'VISCODE','Study Date':'EXAMDATE'}, inplace=True)
        
        aibl_subject_data['VISCODE'] = aibl_subject_data['VISCODE'].apply(lambda x: visit_dictionary[x])
        aibl_subject_data['EXAMDATE'] = pd.to_datetime(aibl_subject_data['EXAMDATE'])
        aibl_subject_data.set_index(['RID','VISCODE'] , inplace=True)
        aibl_merge_data = aibl_subject_data.join(aibl_merge_data,  how='left', rsuffix='_complete')
        aibl_merge_data.to_csv(Path(__file__).parent / f'../../data/AIBL_metadata/aibl_merged_data.csv')