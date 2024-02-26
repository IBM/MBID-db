import os
import sys
from botocore.client import Config
import ibm_boto3
import pandas as pd
from pathlib import Path
from shutil import rmtree
import numpy as np
import json
import re
import nda_helper as ndh
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app
from utils import upload_directory
from config.globals import ENVIRONMENT


###############################################################################
# Input params
###############################################################################

manifest_csv = Path('/data/datasets/HCA_samples/HCPAgingImgManifestBeh/'
                    'datastructure_manifest.txt')
cred_json = 'nda_cred.json'
out_base_dir = Path('/data/datasets/HCA/')
out_csv_dir = Path('/data/datasets/HCA_metadata/')
pkg_id = 1184998
n_processors = 8
do_upload = True  # False
do_downloads = True

###############################################################################
# Load the data from the "manifest" csv
###############################################################################

mdf = pd.read_csv(manifest_csv, sep='\t', skiprows=1)

# Load NDA credentials
with open(cred_json, 'r') as jsfn:
    cred_dict = json.load(jsfn)

###############################################################################
# Make list of T1 images and subject list
###############################################################################

is_t1w = mdf['associated_file'].apply(lambda x: (('_mean.nii.gz' in x) and
                                                 ('T1w' in x)))
t1_series = mdf['associated_file'].loc[is_t1w]
t1_subjs = t1_series.apply(lambda x: x.split('/')[-1].split('_')[0])
t1_subjs.name = 'subjid'

###############################################################################
# Make list of diffusion images
###############################################################################

r = re.compile(".*unprocessed.*dir9[89]_[PA][PA].nii.gz")
is_dwi = mdf['associated_file'].apply(lambda x: bool(r.match(x)))
dwi_series = mdf['associated_file'].loc[is_dwi]
dwi_subjs = dwi_series.apply(lambda x: x.split('/')[4][:-6])
dwi_subjs.name = 'subjid'

# subjid = 'HCA9503576'
# six = np.argwhere(dwi_subjs.values == subjid)[:, 0]
# dwi_nii = dwi_series.iloc[six].values
# dwi_files = {'dwi': dwi_nii,
#              'dwi_json': [fn.split('.')[0] + '.json' for fn in dwi_nii],
#              'dwi_bval': [fn.split('.')[0] + '.bval' for fn in dwi_nii],
#              'dwi_bvec': [fn.split('.')[0] + '.bvec' for fn in dwi_nii]}

###############################################################################
# Make list of rsfMRI images
###############################################################################

r = re.compile(".*unprocessed.*REST[12]_[PA][PA].nii.gz")
is_rsfmri = mdf['associated_file'].apply(lambda x: bool(r.match(x)))
rsfmri_series = mdf['associated_file'].loc[is_rsfmri]
rsfmri_subjs = rsfmri_series.apply(lambda x: x.split('/')[4][:-6])
rsfmri_subjs.name = 'subjid'

###############################################################################
# Merge all into a df and save as csv
###############################################################################

# Makes a df with 3 columns: subjid, modality, and associated file
mod_dict = {'T1': [t1_subjs, t1_series], 'DWI': [dwi_subjs, dwi_series],
            'rsfMRI': [rsfmri_subjs, rsfmri_series]}

merge_mdict = {k: pd.concat(v + [pd.Series(np.full_like(v[0], k),
               index=v[0].index, name='modality')], axis=1)
               for k, v in mod_dict.items()}
merge_df = pd.concat(merge_mdict.values())

# Add rows for all the associated jsons and the bvec and bval files
jdf = merge_df.copy()
jdf['associated_file'] = jdf['associated_file'].\
    apply(lambda x: x.split('.')[0] + '.json')
all_dfs = [merge_df, jdf]
for ext in ['.bval', '.bvec']:
    temp_df = merge_df[merge_df['modality'] == 'DWI'].copy()
    temp_df['associated_file'] = temp_df['associated_file'].\
        apply(lambda x: x.split('.')[0] + ext)
    all_dfs.append(temp_df)

full_df = pd.concat(all_dfs, ignore_index=True)
full_df.sort_values(by=['subjid', 'modality', 'associated_file'], inplace=True)
full_df.reset_index(drop=True, inplace=True)


###############################################################################
# Download files to out_dir
###############################################################################

out_base_dir.mkdir(parents=True, exist_ok=True)

# Make list of subjects and cut in chucks for processing
all_subjs = full_df['subjid'].unique()
chunk_size = 10
n_chunks = int(np.ceil(all_subjs.shape[0]/chunk_size))
subj_chunks = np.array_split(all_subjs, n_chunks)
subjs = subj_chunks[0][:2]  # ['HCA9503576']

tt = full_df['associated_file']
local_filename = pd.Series(np.full_like(tt, ''), index=tt.index,
                           name='local_filename')
full_df['local_filename'] = local_filename

# for subjid in subjs:
#     rows = full_df['subjid'] == subjid
for subjs in subj_chunks:
    rows = full_df['subjid'].isin(subjs)

    dl_fns = tt[rows].values
    if do_downloads:
        # do the download:
        out_fns, fn_exists = ndh.nda_s3_download_file_list(pkg_id, dl_fns,
                                                           out_base_dir,
                                                           cred_dict['username'],
                                                           cred_dict['password'],
                                                           threads=n_processors)
        assert np.all(fn_exists)
        # update the df with the local filenames:
        full_df.loc[rows, 'local_filename'] = [fn.as_posix() for fn in out_fns]

    ###########################################################################
    # Merge the DWIs files:
    ###########################################################################

    # Merges repetitions of dwi and update the df to include all merged files
    # (including nii.gz, bvec, json, etc)
    full_df = ndh.hca_dwi_merge_from_full_df(full_df, subjs,
                                             n_processors=n_processors)

    ###########################################################################
    # Merge the rs-fMRI files:
    ###########################################################################

    #This step is SKIPPED since fMRI repetitions are from different days
    do_rsfmri_merge = False
    if do_rsfmri_merge:
        full_df = ndh.hca_rsfmri_merge_from_full_df(full_df, subjs,
                                                    n_processors=n_processors)
    ###########################################################################
    # Upload images
    ###########################################################################

    if do_upload:
        config = os.environ
        app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
        cos_cred = app.config['COS_CREDENTIALS']
        with app.app_context():
            cos_client =\
                ibm_boto3.client('s3',
                                 ibm_api_key_id=cos_cred['apikey'],
                                 ibm_service_instance_id=cos_cred['resource_instance_id'],
                                 ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                 config=Config(signature_version='oauth'),
                                 endpoint_url=app.config['SERVICE_ENDPOINT'])
        hca_dir = (out_base_dir / 'imagingcollection01').as_posix()
        dataset_name = 'HCA'
        upload_directory(hca_dir, dataset_name, cos_client, app.config['BUCKET'])


    full_df.to_csv(out_csv_dir / 'HCA_cloud_files.csv')
    rmtree(hca_dir)

