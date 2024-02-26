#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 21:22:47 2022

Script to retrieve metadata from PREDICT-HD. Three pieces of information are
extracted: baseline information, longitudinal information (demographics +
clinical assessments) and imaging metadata.

@author: Eduardo Castro
"""

import pandas as pd
import numpy as np
import os
import fnmatch
import glob
from os.path import join as opj

"""
FUNCTION DEFINITIONS
"""

def retrieve_img_fnames(rootdir, pattern='*'):
    """
    Retrieve imaging filenames within the subdirectories in the specified root
    directory

    Parameters
    ----------
    rootdir: str
        Root directory where imaging filenames are located
    patterns: str
        Filename pattern to search (e.g., *.nii.gz)

    Returns
    -------
    img_fnames: list
        List of detected imaging filenames that match the specified pattern
    """
    if not os.path.exists(rootdir):
        raise ValueError("Directory not found {}".format(rootdir))

    img_fnames = []
    for root, dirnames, filenames in os.walk(rootdir):
        for filename in filenames:
            full_path = opj(root, filename)
            if fnmatch.filter([full_path], pattern):
                img_fnames.append(opj(root, filename))
    return img_fnames


def img_df_generator(img_fnames):
    """
    Populate a dataframe with information contained within the structure of the
    available imaging filenames for the PREDICT-HD dataset

    Parameters
    ----------
    img_fnames: list
        List of imaging filenames

    Returns
    -------
    img_df: pandas DataFrame
        Dataframe with fields associated to imaging metadata
    """
    
    # Retrieve imaging info from filenames
    img_df = pd.DataFrame(data=img_fnames, columns=['full_fname'])
    img_df['fname'] = img_df['full_fname'].apply(lambda x: x.split('/')[-1])
    img_df['subjid'] = img_df['fname'].apply(lambda x: x.split('_')[0])
    img_df['scan_id'] = img_df['fname'].apply(lambda x: x.split('_')[1])
    img_df[['modality', 'field_st']]=\
        img_df['fname'].str.split('_', expand=True).iloc[:, 2].str.\
            split('-', expand=True)
    img_df['series'] = img_df['fname'].apply(lambda x: x.split('_')[3])

    # Additional formatting to columns
    img_df['subjid'] = img_df['subjid'].map('{:0>6}'.format)
    img_df['field_st'] = img_df['field_st'].astype(int)
    img_df['scan_id'] = img_df['scan_id'].astype(int)
    del img_df['fname']

    return img_df
    

def generate_predict_tables(input_dir, fn_img_df):
    """
    Function to generate 3 dataframes: baseline, longitudinal and imaging
    metadata. 

    Parameters
    ----------
    input_dir: str
        Directory where spreadsheets provided by CHDI are located
    fn_img_df: pandas.DataFrame
        Ddataframe with information retrieved from the scans
        filenames in imaging directories (chdi disks in /data1)
    
    Returns
    -------
    baseline_df: pandas DataFrame
        Dataframe with demographic information at baseline visit
    longit_df:  pandas DataFrame
        Dataframe with longitudinal clinical information
    imaging_df: pandas DataFrame
        Dataframe with imaging metadata and imaging files location
    
    """
    cols_img_all = ['dbgap_id', 'siteid', 'field_st', 'scanType', 'series',
                    'visdy', 'tr', 'te', 'flip', 'voxelResX', 'voxelResY',
                    'voxelResZ', 'manufacturer', 'model', 'hdcat', 'scan_id']
    
    cols_general = ['dbgap_id', 'visdy', 'siteid', 'age', 'sex', 'race_nih']
    cols_clinical = ['dbgap_id', 'visdy', 'sdmt1', 'tfcscore', 'motscore',
                     'bmi', 'hdcat']
    cols_subj = ['dbgap_id', 'caghigh', 'hdcat_l']

    cols_map = {'dbgap_id': 'subjid', 'scanType': 'modality',
                'hdcat': 'group', 'caghigh': 'CAG'}
    
    hdvals_map = {1: 'unknown', 2: 'preHD', 3: 'earlyHD', 4: 'control',
                  5: 'control', 6: 'control'}
    
    race_map = {1: 'american_indian', 2: 'asian_or_pacific',
                3: 'asian_or_pacific', 4: 'black', 5: 'white_non_latino',
                6: 'multiple', 7: 'undefined'}
        
    # Retrieve fields from original imaging spreadsheet
    orig_img_fn = glob.glob(opj(input_dir, 'images_sMRI*.csv'))[0]
    orig_img_df = pd.read_csv(orig_img_fn, usecols=cols_img_all)
    orig_img_df['dbgap_id'] = orig_img_df['dbgap_id'].map('{:0>6}'.format)
    orig_img_df['field_st'] = (orig_img_df['field_st'] * 10).astype(int)
    orig_img_df = orig_img_df.drop_duplicates()
    
    # Rename multiple variables
    orig_img_df.rename(columns=cols_map, inplace=True)
    orig_img_df.replace({'group': hdvals_map}, inplace=True)
    
    # Merge both imaging dataframes (entries from csv file and img files per se)
    orig_img_df =\
        fn_img_df.merge(orig_img_df, how='inner',
                        on=list(fn_img_df.columns.difference(['full_fname'])))
    
    # Keep only T1W scans from 3 T scanners
    orig_img_df = orig_img_df[(orig_img_df.field_st == 30) &
                              (orig_img_df.modality == 'T1')]
    
    
    # Retrieve general info fields
    general_fn = glob.glob(opj(input_dir, 'General*.csv'))[0]
    general_df = pd.read_csv(general_fn, usecols=cols_general)
    general_df.replace({'race_nih': race_map}, inplace=True)
    general_df.rename(columns=cols_map, inplace=True)
    general_df['subjid'] = general_df['subjid'].map('{:06d}'.format)
    
    # assign visit number
    general_df.sort_values(by=['subjid', 'visdy'], inplace=True)
    general_df['visit'] =\
        general_df.groupby('subjid')['visdy'].apply(lambda x:
                                                    (x != x.shift(1)).cumsum())

    # Add visit number info to imaging dataframe and
    inter_cols = ['subjid', 'visdy']
    imaging_df = orig_img_df.merge(general_df[inter_cols + ['visit']],
                                   how='left', on=inter_cols)
    imaging_df.sort_values(['subjid', 'visdy'], inplace=True)
    
    
    # Get CAG repeats from subject dataframe
    subject_fn = glob.glob(opj(input_dir, 'subjects*.csv'))[0]
    subject_df = pd.read_csv(subject_fn, usecols=cols_subj)
    subject_df.columns = subject_df.columns.str.replace('_l', '')
    subject_df.rename(columns=cols_map, inplace=True)
    subject_df['subjid'] = subject_df['subjid'].map('{:06d}'.format)
    subject_df.replace({'group': hdvals_map}, inplace=True)
    
    # Set CAG in controls to NaN
    subject_df.loc[subject_df.group == 'control', 'CAG'] = np.nan
    
    # Generate baseline information dataframe
    baseline_df = general_df[general_df.visdy == 0]
    del baseline_df['visit']
    baseline_df = baseline_df.merge(subject_df, on='subjid', how='inner')
    
    
    # Get clinical scores (baseline and follow-up)
    clinical_fn = glob.glob(opj(input_dir, 'baseline*.csv'))[0]
    base_df = pd.read_csv(clinical_fn, usecols=cols_clinical)
    clinical_fn = glob.glob(opj(input_dir, 'follow-up*.csv'))[0]
    follup_df = pd.read_csv(clinical_fn, usecols=cols_clinical)
    
    clinical_df = pd.concat([base_df, follup_df])
    clinical_df.rename(columns=cols_map, inplace=True)
    clinical_df.replace({'group': hdvals_map}, inplace=True)
    clinical_df.sort_values(['subjid', 'visdy'], inplace=True)
    clinical_df = clinical_df.drop_duplicates()
    clinical_df['subjid'] = clinical_df['subjid'].map('{:06d}'.format)
    
    # Incorporate visit and age from general df; add CAG, estimate CAP
    clin_cols = list(clinical_df.columns.difference(general_df.columns)) +\
        inter_cols
    gen_cols = inter_cols + ['visit', 'age']
    longit_df = general_df[gen_cols].merge(clinical_df[clin_cols], how='inner',
                                 on=inter_cols)

    longit_df = longit_df.merge(subject_df[['subjid', 'CAG']],
                                on='subjid', how='inner')
    longit_df['CAP'] = (longit_df['CAG'] - 30)/627 * 100 * longit_df['age']
    del longit_df['CAG']
    
    # For imaging, longitudinal and baseline dataframes, discard TRACK-HD participants
    overlap_subjid_fn = glob.glob(opj(input_dir, 'dbGAPID*.xlsx'))[0]
    overlap_subjid_df = pd.read_excel(overlap_subjid_fn, engine='openpyxl')
    overlap_subjid_df['dbgapID'] = overlap_subjid_df['dbgapID'].map('{:06d}'.format)
    TRACK_subjid = overlap_subjid_df.dbgapID.values
    
    baseline_df = baseline_df[~baseline_df['subjid'].isin(TRACK_subjid)]
    longit_df = longit_df[~longit_df['subjid'].isin(TRACK_subjid)]
    imaging_df = imaging_df[~imaging_df['subjid'].isin(TRACK_subjid)]

    return baseline_df, longit_df, imaging_df


"""
MAIN SCRIPT
"""

if __name__ == '__main__':

    # Directories with relevant information from PREDICT-HD
    metadir = '/data1/chdi_disks/updated_metadata_Predict'
    img_dir1 = '/data1/chdi_disks/Disk3/PREDICT-HD/imaging_data'
    img_dir2 = '/data1/chdi_disks/Disk6/data'
    store_dir = '/data2/eduardo/code/xls_preprocessing/data/PREDICT-HD_metadata'
    
    # Retrieve imaging info from filenames
    img_fnames1 = retrieve_img_fnames(img_dir1, pattern='*defaced.nii.gz')
    fn_img_df1 = img_df_generator(img_fnames1)
    img_fnames2 = retrieve_img_fnames(img_dir2, pattern='*defaced.nii.gz')
    fn_img_df2 = img_df_generator(img_fnames2)

    fn_img_df = pd.concat([fn_img_df1, fn_img_df2])
    fn_img_df = fn_img_df.drop_duplicates(
        subset=fn_img_df.columns.difference(['full_fname']))
    
    # Retrieve baseline, longitudinal and imaging metadata dataframes; store
    baseline_df, longit_df, imaging_df = generate_predict_tables(metadir,
                                                                 fn_img_df)
    imaging_df.to_csv(opj(store_dir, 'imaging.csv'), index=False)
    baseline_df.to_csv(opj(store_dir, 'baseline.csv'), index=False)
    longit_df.to_csv(opj(store_dir, 'longitudinal.csv'), index=False)
