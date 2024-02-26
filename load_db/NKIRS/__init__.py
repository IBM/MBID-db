import pandas as pd

def open_subject_metadata_file(app, cos_client, subject_external_id):
    try:
        # First try to read the subject's file
        subject_csv = f'NKIRS/{subject_external_id}/{subject_external_id}_sessions.tsv'
        response = cos_client.Object(app.config['BUCKET'], subject_csv).get()
        subject_info_df = pd.read_csv(response.get("Body"), sep='\t')
        subject_info_df.set_index('session', inplace=True)
        return subject_info_df
    except:
        print(f'Error while trying to read metadata file for subject {subject_external_id}')
        return None

def get_baseline_visit(subject_info_df):
    # The baseline visit is the one with the Highest number (BAS3 > BAS2 > BAS1)
    if 'BAS3' in subject_info_df:
        return 'BAS3'
    elif 'BAS2' in subject_info_df:
        return 'BAS2'
    else:
        return 'BAS1'