#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  6 23:23:38 2021

This script is used primarily to retrieve useful information from PPMI for the
XLS database. Together with *imaging_file_prunning.py*, it provides all
necessary code to have PPMI ready to be uploaded to the cloud.

Another part of the script (secondary one) is meant to retrieve behavioral
measures from PPMI (PyPMI package: https://github.com/rmarkello/pypmi).
While this is not necessary to shape the database, information from that
package was used to guide PPMI data retrieval.

@author: Eduardo Castro
"""

import pypmi
import numpy as np
import pandas as pd
from os.path import join as opj


# *** PART 1: PyPMI package exploration

# Load behavioral data
data_path = '/data2/eduardo/datasets/PPMI/metadata'
beh_measures = ['benton', 'education', 'epworth', 'gds', 'hvlt_recall',
                'hvlt_recognition', 'hvlt_retention', 'lns', 'moca', 'pigd',
                'quip', 'rbd', 'scopa_aut', 'se_adl', 'semantic_fluency',
                'stai_state', 'stai_trait', 'symbol_digit', 'systolic_bp_drop',
                'tremor', 'updrs_i', 'updrs_ii', 'updrs_iii', 'updrs_iii_a',
                'updrs_iv']
behavior = pypmi.load_behavior(path=data_path, measures=beh_measures)

# Store compiled features
behavior.to_csv('/data2/eduardo/datasets/PPMI/metadata/compiled_features'
                '/behavior_table.csv')


# *** PART 2: Demographic and behavioral data location in PPMI (files and cols)
metadir = '/data2/eduardo/datasets/PPMI/metadata'
derived_dir = '/data2/eduardo/datasets/PPMI/metadata/compiled_features'

# Montreal Cognitive Assessment Test
moca_dict = {
    'files': {
        opj(metadir, 'Montreal_Cognitive_Assessment__MoCA_.csv'): 
            ['PATNO', 'EVENT_ID'] + \
            ['MCAALTTM', 'MCACUBE', 'MCACLCKC', 'MCACLCKN', 'MCACLCKH',
             'MCALION', 'MCARHINO', 'MCACAMEL', 'MCAFDS', 'MCABDS',
             'MCAVIGIL', 'MCASER7', 'MCASNTNC', 'MCAVF', 'MCAABSTR',
             'MCAREC1', 'MCAREC2', 'MCAREC3', 'MCAREC4', 'MCAREC5',
             'MCADATE', 'MCAMONTH', 'MCAYR', 'MCADAY', 'MCAPLACE',
             'MCACITY']
    },
}

# Symbol Digit Modalities Test
sdmt_dict = {
    'files': {
        opj(metadir, 'Symbol_Digit_Modalities_Test.csv'):
            ['PATNO', 'EVENT_ID', 'SDMTOTAL']
    }
}

# Tremor
tremor_dict = {
    'files': {
        opj(metadir, 'MDS_UPDRS_Part_II__Patient_Questionnaire.csv'):
            ['PATNO', 'EVENT_ID', 'NP2TRMR'],
        opj(metadir, 'MDS_UPDRS_Part_III.csv'):
            ['PATNO', 'EVENT_ID'] + \
            ['NP3PTRMR', 'NP3PTRML', 'NP3KTRMR', 'NP3KTRML', 'NP3RTARU',
             'NP3RTALU', 'NP3RTARL', 'NP3RTALL', 'NP3RTALJ', 'NP3RTCON']
    }
}

# Postural instability and gait disorders
pigd_dict = {
    'files': {
        opj(metadir, 'MDS_UPDRS_Part_II__Patient_Questionnaire.csv'):
            ['PATNO', 'EVENT_ID', 'NP2WALK', 'NP2FREZ'],
        opj(metadir, 'MDS_UPDRS_Part_III.csv'):
            ['PATNO', 'EVENT_ID', 'NP3GAIT', 'NP3FRZGT', 'NP3PSTBL']
    }
}

# UPDRS 1: Mentation, Behavior, and Mood
updrs1_dict = {
    'files': {
        opj(metadir, 'MDS-UPDRS_Part_I.csv'): 
            ['PATNO', 'EVENT_ID'] + \
            ['NP1COG', 'NP1HALL', 'NP1DPRS', 'NP1ANXS', 'NP1APAT',
             'NP1DDS'],
        opj(metadir, 'MDS-UPDRS_Part_I_Patient_Questionnaire.csv'): 
            ['PATNO', 'EVENT_ID'] + \
            ['NP1SLPN', 'NP1SLPD', 'NP1PAIN', 'NP1URIN', 'NP1CNST',
             'NP1LTHD', 'NP1FATG']
    }
}

# UPDRS 2: Activities of Daily Living
updrs2_dict = {
    'files': {
        opj(metadir, 'MDS_UPDRS_Part_II__Patient_Questionnaire.csv'):
            ['PATNO', 'EVENT_ID'] + \
            ['NP2SPCH', 'NP2SALV', 'NP2SWAL', 'NP2EAT', 'NP2DRES',
             'NP2HYGN', 'NP2HWRT', 'NP2HOBB', 'NP2TURN', 'NP2TRMR',
             'NP2RISE', 'NP2WALK', 'NP2FREZ']
    }
}

# UPDRS 3: Motor
updrs3_dict = {
    'files': {
        opj(metadir, 'MDS_UPDRS_Part_III.csv'):
            ['PATNO', 'EVENT_ID', 'PDSTATE'] + \
            ['NP3SPCH', 'NP3FACXP', 'NP3RIGN', 'NP3RIGRU', 'NP3RIGLU',
             'NP3RIGRL', 'NP3RIGLL', 'NP3FTAPR', 'NP3FTAPL', 'NP3HMOVR',
             'NP3HMOVL', 'NP3PRSPR', 'NP3PRSPL', 'NP3TTAPR', 'NP3TTAPL',
             'NP3LGAGR', 'NP3LGAGL', 'NP3RISNG', 'NP3GAIT', 'NP3FRZGT',
             'NP3PSTBL', 'NP3POSTR', 'NP3BRADY', 'NP3PTRMR', 'NP3PTRML',
             'NP3KTRMR', 'NP3KTRML', 'NP3RTARU', 'NP3RTALU', 'NP3RTARL',
             'NP3RTALL', 'NP3RTALJ', 'NP3RTCON']
    }
}

# UPDRS 4: Modified Hoehn and Yahr Scale
updrs4_dict = {
    'files': {
        opj(metadir, 'MDS-UPDRS_Part_IV__Motor_Complications.csv'):
            ['PATNO', 'EVENT_ID'] + \
            ['NP4WDYSK', 'NP4DYSKI', 'NP4OFF', 'NP4FLCTI', 'NP4FLCTX',
             'NP4DYSTN']
    }
}

# Vital signs
vital_dict = {
    'files': {
        opj(metadir, 'Vital_Signs.csv'):
            ['PATNO', 'EVENT_ID'] + \
            ['WGTKG', 'HTCM']
    }
}

# DEMOGRAPHICS

# baseline info (diagnostic and age at baseline) 
base_dict = {
    'files': {
        opj(metadir, 'Participant_Status.csv'):
            ['PATNO', 'CONCOHORT', 'ENROLL_STATUS', 'ENROLL_AGE']
    }
}

diagn_map = {1: "Parkinson's Disease", 2: 'Healthy Control', 4: 'Prodromal PD',
             0: 'exclude'}

# age at visit
age_vis_dict = {
    'files': {
        opj(metadir, 'Age_at_visit.csv'):
            ['PATNO', 'EVENT_ID', 'AGE_AT_VISIT']
    }
}

# race
demog_dict = {
    'files': {
        opj(metadir, 'Screening___Demographics-Archived.csv'):
            ['PATNO', 'GENDER'] + \
            ['RAINDALS', 'RAASIAN', 'RABLACK', 'RAHAWOPI', 'RAWHITE', 'RANOS']
    }
}

race_map = {'RAASIAN': 'asian_or_pacific',
            'RABLACK': 'black',
            'RAHAWOPI': 'asian_or_pacific',
            'RAINDALS': 'american_indian',
            'RAWHITE': 'white_non_latino',
            'RANOS': 'undefined'}

gender_map = {0: 'female', 1: 'female', 2: 'male', np.nan: 'undefined'}

# RAASIAN	Asian
# RABLACK	Black
# RAHAWOPI	Hawaiian/Other Pacific Islander
# RAINDALS	American Indian/Alaska Native
# RANOS		Race not specified
# RAWHITE	White

# education
education_dict = {
    'files': {
        opj(metadir, 'Socio-Economics.csv'):
            ['PATNO', 'EVENT_ID', 'EDUCYRS']
    }
}

# visits (conventional visit 1, 2, 3; no BL SC, etc.)
visit_dict = {
    'files': {
        opj(metadir, 'Vital_Signs.csv'):
            ['PATNO', 'EVENT_ID', 'INFODT']
    }
}

vm1 = {'SC': 1, 'BL': 1}
nvis = 20
vm2 = dict(zip([f'V{i:02}' for i in range(1, nvis+1)], range(2, nvis+2)))
visit_map = {**vm1, **vm2}

# imaging (already massaged csv generated in 'imaging_files_prunning.py')
img_dict = {
    'files': {
        ('/data2/eduardo/datasets/PPMI/nifti/imaging_info/'
         'all_clear_img_fnames_cloud-db.csv'):
            ['subjid', 'modality', 'date', 'fname']
    }
}

    
# *** PART 3: Additional imaging metadata

T1_json = {'SeriesDescription': '3D T1-weighted',
           'SliceThickness': 1.0,
           'BaseResolution': 256, # other field of matrix size?
           'MRAcquisitionType': '3D',
           "MagneticFieldStrength": 3}
# Note: 1. Missing phase encoding direction (AP, but RAS?)
#       2. Since SAG ACQ, slice thickness should be retrieved with
#          img.header.get_zooms()[0] (next time)

bval = ['0', '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000',
        '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000',
        '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000',
        '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000',
        '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000',
        '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000',
        '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000',
        '1000', '1000']

bvec0 = np.array([ 0.       ,  0.999975 ,  0.       , -0.0249489,  0.589353 ,
                   -0.235876 , -0.893575 ,  0.797989 ,  0.232856 ,  0.936737 ,
                    0.50383  ,  0.34488  ,  0.456944 , -0.488004 , -0.616849 ,
                   -0.578618 , -0.82544  ,  0.895346 ,  0.290066 ,  0.114907 ,
                   -0.800098 ,  0.511672 , -0.789853 ,  0.949255 ,  0.231806 ,
                   -0.019682 ,  0.215852 ,  0.772394 , -0.160407 , -0.147005 ,
                    0.886886 , -0.563141 , -0.380874 , -0.306491 , -0.332448 ,
                   -0.961969 , -0.959512 ,  0.450657 , -0.77098  ,  0.709698 ,
                   -0.695219 ,  0.681197 , -0.14117  , -0.740626 , -0.102813 ,
                    0.583876 , -0.0869765, -0.550628 ,  0.837193 ,  0.362462 ,
                   -0.183761 , -0.717975 ,  0.432372 ,  0.501455 , -0.170594 ,
                    0.463392 ,  0.383808 , -0.714437 ,  0.259001 ,  0.       ,
                    0.0363685,  0.57092  , -0.282461 ,  0.720033 ,  0.265646 ])

bvec1 = np.array([ 0.        , -0.00504502,  0.999988  ,  0.65464   , -0.769866  ,
                   -0.529835  , -0.264756  ,  0.133987  ,  0.932211  ,  0.145261  ,
                   -0.847151  , -0.850958  , -0.63592   , -0.39473   ,  0.677327  ,
                   -0.109633  , -0.525215  , -0.0448607 , -0.546911  , -0.964118  ,
                    0.408673  ,  0.84275   ,  0.158242  , -0.238637  ,  0.78726   ,
                   -0.193328  , -0.957363  , -0.608092  ,  0.361349  ,  0.73598   ,
                    0.422508  ,  0.237351  ,  0.148151  , -0.204151  , -0.13535   ,
                   -0.270561  ,  0.21075   , -0.890558  ,  0.631619  ,  0.413843  ,
                    0.0287264 ,  0.534165  , -0.730096  ,  0.393661  ,  0.82609   ,
                   -0.60135   , -0.340311  , -0.795773  , -0.463276  , -0.566938  ,
                    0.397406  , -0.696063  ,  0.68709   ,  0.695404  , -0.514517  ,
                    0.428668  , -0.813147  , -0.252685  ,  0.887447  ,  0.0814826 ,
                    -0.905214  , -0.309049  ,  0.149931  ,  0.612825  ,  0.960757  ])

bvec2 = np.array([ 0.        , -0.00503994, -0.00497989, -0.755529  , -0.244886  ,
                   -0.81464   , -0.36253   , -0.58759   , -0.277056  , -0.31847   ,
                    0.168788  ,  0.396143  ,  0.621939  , -0.778486  , -0.400906  ,
                    0.808197  , -0.206878  ,  0.443106  ,  0.785335  ,  0.239319  ,
                    0.439123  , -0.167226  ,  0.59253   , -0.20486   , -0.571391  ,
                    0.980937  , -0.191999  , -0.183388  , -0.91853   ,  0.66085   ,
                   -0.186867  ,  0.791541  , -0.912681  ,  0.929723  , -0.933359  ,
                   -0.0375901 , -0.186872  , -0.0617694 , -0.0815332 , -0.570143  ,
                   -0.718224  ,  0.500638  , -0.668603  , -0.544521  , -0.554081  ,
                   -0.545406  , -0.936282  , -0.252098  ,  0.290659  , -0.739731  ,
                    0.899055  , -0.00282879,  0.583919  , -0.514739  ,  0.840339  ,
                   -0.775572  , -0.437588  , -0.65248   ,  0.381257  ,  0.996675  ,
                   -0.423396  ,  0.760617  ,  0.94749   , -0.325574  ,  0.0798617 ])

bvec = [[bvec0, bvec1, bvec2]]

np.savez(opj(derived_dir, 'DTI_extra_params.npz'), bval=bval, bvec=bvec)
# Note: Values retrieved from files generated after DCM2BIDS (py2pmi function)

DWI_json = {"MagneticFieldStrength": 3,
            'SliceThickness': 2.0,
            'BaseResolution': 128, # other field of matrix size?
            'RepetitionTime': 10,
            'EchoTime': 0.08,
            'FlipAngle': 90,
            'bval': bval,
            'bvec': bvec}
# Note: missing slice order info + PE direction

func_json = {'SeriesDescription': 'rsfMRI_RL',
             'SliceThickness': 3.5,
             'BaseResolution': 64, # other field of matrix size?
             'RepetitionTime': 2.5,
             'EchoTime': 0.03}
# Note: missing slice order info + PE direction


# *** PART 4: Retrieval of PPMI info into 3 df: demographics, clinical and imaging

# 1. Put together final imaging csv

[(img_fn, img_cols)] = img_dict['files'].items()
img_df = pd.read_csv(img_fn, usecols=img_cols)

# rename modalities (T1, DWI, rsfMRI)
img_df.loc[img_df['modality'].str.contains('MPRAGE|FSPGR|T1',
                                           case=False, regex=True),
           'modality'] = 'T1'

img_df.loc[img_df['modality'].str.contains('DTI', case=False),
           'modality'] = 'DWI'

img_df.loc[img_df['modality'].str.contains('rsfMRI|rest',
                                           case=False, regex=True),
           'modality'] = 'rsfMRI'

# date formatting and column replacing
img_df['date'] = img_df['date'].apply(lambda x: x.split('_')[0])
img_df['date'] = pd.to_datetime(img_df['date'])
img_map = {'subjid': 'PATNO',
           'date': 'INFODT'}
img_df.rename(columns=img_map, inplace=True)

# get visits info; assign addl vars (e.g., days from baseline, visit index, etc.)
[(vst_fn, vst_cols)] = visit_dict['files'].items()
vst_df = pd.read_csv(vst_fn, usecols=vst_cols)
vst_df['INFODT'] = pd.to_datetime(vst_df['INFODT'])

# add visit entry (1, 2, 3, etc.) and discard weird visits (U01, ST, etc.)
vst_df = vst_df[vst_df['EVENT_ID'].isin(list(visit_map.keys()))]
vst_df['visit'] = vst_df['EVENT_ID'].map(visit_map).astype(int)

# days from baseline
idx_base = vst_df.groupby('PATNO')['INFODT'].\
    transform(lambda x: x.nsmallest(2).idxmax())
vst_df['days_since_baseline'] = vst_df.loc[idx_base, 'INFODT'].values
vst_df['days_since_baseline'] = (vst_df['INFODT'] -
                                vst_df['days_since_baseline']).dt.days

# Merge imaging csv with visit one (get visit number and days_baseline from it)
subset_cols = ['INFODT', 'visit', 'days_since_baseline']
img_extended_df = pd.DataFrame()
for subjid in img_df.PATNO.unique():
    img_subj_df = img_df[img_df.PATNO == subjid].sort_values('INFODT')
    vst_subj_df = vst_df[vst_df.PATNO == subjid].sort_values('INFODT')
    img_subj_df = pd.merge_asof(left=img_subj_df, right=vst_subj_df[subset_cols],
                                on='INFODT', direction='nearest')
    img_extended_df = img_extended_df.append(img_subj_df)

# Correct indexes and store updated imaging dataframe
sort_img_cols = ['PATNO', 'modality', 'visit', 'INFODT', 'days_since_baseline',
                 'fname']
img_extended_df.reset_index(drop=True, inplace=True)
img_extended_df = img_extended_df.sort_values(['PATNO', 'visit'])
img_extended_df = img_extended_df[sort_img_cols]

 
# 2. Put together final subject csv (baseline)

# core baseline info
[(base_fn, base_cols)] = base_dict['files'].items()
base_df = pd.read_csv(base_fn, usecols=base_cols)
base_df.replace({'CONCOHORT': diagn_map}, inplace=True)

# give columns decent names
base_map = {'ENROLL_STATUS': 'status',
            'ENROLL_AGE': 'age',
            'CONCOHORT': 'condition'}
base_df.rename(columns=base_map, inplace=True)

# discard excluded subjects (and invalid diagnosis ones) from image and visit dfs
base_df = base_df.dropna(subset=['condition'])
base_df['condition'] = base_df['condition'].astype(str)
base_df = base_df[base_df['condition'].str.contains("Parkinson's Disease|"
                                                    "Healthy Control|Prodromal PD",
                                                    regex=True)]
del base_df['status']

subjid_all = base_df['PATNO'].unique()
img_extended_df = img_extended_df[img_extended_df['PATNO'].isin(subjid_all)]
img_extended_df.to_csv(opj(derived_dir, 'img_db_ready.csv'), index=False)
vst_df = vst_df[vst_df['PATNO'].isin(subjid_all)]
vst_df.to_csv(opj(derived_dir, 'visit_date_info.csv'), index=False)

# core demographics info (race inclusive)
[(demog_fn, demog_cols)] = demog_dict['files'].items()
demog_df = pd.read_csv(demog_fn, usecols=demog_cols)

demog_df[['numb_race', 'race']] = np.nan
demog_df['numb_race'] = demog_df[race_map.keys()].sum(axis=1)
demog_df.loc[demog_df['numb_race'] > 1, 'race'] = 'multiple'
for rkey, rval in race_map.items():
    demog_df.loc[(demog_df[rkey] == 1) &
                 (demog_df['numb_race'] == 1), 'race'] = rval
demog_df.loc[demog_df['numb_race'] == 0, 'race'] = 'undefined'

# get rid of unnecessary columns in demog; edit gender values
demog_df = demog_df.drop(list(race_map.keys()) + ['numb_race'], axis = 1)
demog_df.replace({'GENDER': gender_map}, inplace=True)
demog_df.rename(columns={'GENDER': 'gender'}, inplace=True)

# Retrieve education info (only from baseline)
[(educ_fn, educ_cols)] = education_dict['files'].items()
educ_df = pd.read_csv(educ_fn, usecols=educ_cols)

educ_df = educ_df[educ_df['EVENT_ID'] == 'SC']
del educ_df['EVENT_ID']
educ_df.rename(columns={'EDUCYRS': 'educ_yrs'}, inplace=True)

# Put all baseline info together; hold only valid subjects
demog_df = demog_df.merge(base_df, how='inner', on='PATNO')
demog_df = demog_df.merge(educ_df, how='inner', on='PATNO')
demog_df = demog_df[demog_df['PATNO'].isin(subjid_all)]
demog_df.to_csv(opj(derived_dir, 'demographics_info.csv'), index=False)


# 3. Put together final clinical info csv (visit)

# Clean demographics dataframe (only leave necessary columns)
demog_df = demog_df[['PATNO', 'condition', 'educ_yrs']]

# Get MoCA scores
[(moca_fn, moca_cols)] = moca_dict['files'].items()
moca_df = pd.read_csv(moca_fn, usecols=moca_cols)

moca_scores_nm = list(np.setdiff1d(moca_cols, ['PATNO', 'EVENT_ID']))
moca_df['moca_total'] = moca_df[moca_scores_nm].sum(axis=1)
moca_df = moca_df.drop(moca_scores_nm, axis=1)

# Assign MoCA score from screen visit to baseline one (disregard SC visit)
moca_df.loc[moca_df['EVENT_ID'] == 'SC', 'EVENT_ID'] = 'BL'

# Assign proper visit (and others) to MoCA scores + education info
vst_merge_cols = ['PATNO', 'EVENT_ID', 'visit', 'days_since_baseline']
longit_df = vst_df[vst_merge_cols].merge(moca_df,
                                         how='left', on=['PATNO', 'EVENT_ID'])
longit_df = longit_df.merge(demog_df, how='left', on=['PATNO'])

# Do MoCA adjustment based on education
idx_adj = (longit_df['educ_yrs'] <=12) & (longit_df['moca_total'] < 30)
longit_df.loc[idx_adj, 'moca_total'] += 1

# Get age per visit
[(age_fn, age_cols)] = age_vis_dict['files'].items()
age_df = pd.read_csv(age_fn, usecols=age_cols)
longit_df = longit_df.merge(age_df, how='left', on=['PATNO', 'EVENT_ID'])
longit_df.rename(columns={'AGE_AT_VISIT': 'age'}, inplace=True)

# Get UPDRS 1
updrs1_df = [pd.read_csv(fn, usecols=cols)
             for (fn, cols) in updrs1_dict['files'].items()]
updrs1_df = updrs1_df[0].merge(updrs1_df[1], how='inner',
                               on=['PATNO', 'EVENT_ID'])

# Get rid of strings as scores, reformat. Note: UR: unable to rate
updrs_cols = list(np.setdiff1d(updrs1_df.columns, ['PATNO', 'EVENT_ID']))
updrs1_df.replace({col: {'UR': np.nan}
                   for col in updrs_cols if updrs1_df.dtypes[col] == object},
                  inplace=True)
for col in updrs_cols:
    updrs1_df[col] = updrs1_df[col].astype(float)

updrs1_df['updrs1'] = updrs1_df[updrs_cols].sum(axis=1)
updrs1_df = updrs1_df.drop(updrs_cols, axis=1)

# Get UPDRS 2
[(updrs_fn, updrs_cols)] = updrs2_dict['files'].items()
updrs2_df = pd.read_csv(updrs_fn, usecols=updrs_cols)

updrs_cols = list(np.setdiff1d(updrs2_df.columns, ['PATNO', 'EVENT_ID']))
updrs2_df['updrs2'] = updrs2_df[updrs_cols].sum(axis=1).astype(float)
updrs2_df = updrs2_df.drop(updrs_cols, axis=1)

# Get UPDRS 3
[(updrs_fn, updrs_cols)] = updrs3_dict['files'].items()
updrs3_df = pd.read_csv(updrs_fn, usecols=updrs_cols)

# Get rid of strings as scores, reformat. Note: UR: unable to rate
total_cols = ['updrs3_on', 'updrs3_off', 'updrs3_unk']
updrs3_df[total_cols] = np.nan
updrs_cols = list(np.setdiff1d(updrs3_df.columns, ['PATNO', 'EVENT_ID',
                                                   'PDSTATE'] + total_cols))
updrs3_df.replace({col: {'UR': np.nan}
                   for col in updrs_cols if updrs3_df.dtypes[col] == object},
                  inplace=True)
for col in updrs_cols:
    updrs3_df[col] = updrs3_df[col].astype(float)

# Estimate total values for ON and OFF (and unknown) states separately
on_rows_idx = updrs3_df['PDSTATE'] == 'ON'
updrs3_df.loc[on_rows_idx, 'updrs3_on'] =\
    updrs3_df.loc[on_rows_idx, updrs_cols].sum(axis=1).astype(float)

off_rows_idx = updrs3_df['PDSTATE'] == 'OFF'
updrs3_df.loc[off_rows_idx, 'updrs3_off'] =\
    updrs3_df.loc[off_rows_idx, updrs_cols].sum(axis=1).astype(float)

unk_rows_idx = updrs3_df['PDSTATE'].isnull()
updrs3_df.loc[unk_rows_idx, 'updrs3_unk'] =\
    updrs3_df.loc[unk_rows_idx, updrs_cols].sum(axis=1).astype(float)

updrs3_df = updrs3_df.drop(updrs_cols + ['PDSTATE'], axis=1)

# Get UPDRS 4
[(updrs_fn, updrs_cols)] = updrs4_dict['files'].items()
updrs4_df = pd.read_csv(updrs_fn, usecols=updrs_cols)

# Get rid of strings as scores, reformat. Note: UR: unable to rate
updrs_cols = list(np.setdiff1d(updrs4_df.columns, ['PATNO', 'EVENT_ID']))
updrs4_df.replace({col: {'UR': np.nan}
                   for col in updrs_cols if updrs4_df.dtypes[col] == object},
                  inplace=True)
for col in updrs_cols:
    updrs4_df[col] = updrs4_df[col].astype(float)

updrs4_df['updrs4'] = updrs4_df[updrs_cols].sum(axis=1)
updrs4_df = updrs4_df.drop(updrs_cols, axis=1)

# Merge all UPDRS dataframes, get overall total score
updrs_df = updrs1_df.merge(updrs2_df, how='outer', on=['PATNO', 'EVENT_ID'])
updrs_df = updrs_df.merge(updrs3_df, how='outer', on=['PATNO', 'EVENT_ID'])
updrs_df = updrs_df.merge(updrs4_df, how='outer', on=['PATNO', 'EVENT_ID'])

# Merge UPDRS total score with clinical longitudinal dataframe
longit_df = longit_df.merge(updrs_df, how='left', on=['PATNO', 'EVENT_ID'])

# Get SDMT scores; merge with longit_df, complete this at last
[(sdmt_fn, sdmt_cols)] = sdmt_dict['files'].items()
sdmt_df = pd.read_csv(sdmt_fn, usecols=sdmt_cols)

longit_df = longit_df.merge(sdmt_df, how='left', on=['PATNO', 'EVENT_ID'])

# Get BMI for each visit (when available), merge with longit_df
[(vital_fn, vital_cols)] = vital_dict['files'].items()
vital_df = pd.read_csv(vital_fn, usecols=vital_cols)

vital_df['HTCM'] = vital_df['HTCM'] / 100
vital_df['bmi'] = vital_df['WGTKG'] / (vital_df['HTCM'] ** 2)
vital_df = vital_df.drop(['HTCM', 'WGTKG'], axis=1)

longit_df = longit_df.merge(vital_df, how='left', on=['PATNO', 'EVENT_ID'])

# Remove redundant entries, EVENT_ID (not needed) and any instance of SC visit
longit_df.drop_duplicates(inplace=True)
longit_df = longit_df[longit_df['EVENT_ID'] != 'SC']
del longit_df['EVENT_ID']
longit_df.rename(columns={'SDMTOTAL': 'sdmt'}, inplace=True)

longit_df.to_csv(opj(derived_dir, 'longit_clinical_info.csv'), index=False)
