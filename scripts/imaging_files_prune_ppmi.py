#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 21:24:41 2021

Script to prune the original set of brain imaging files from PPMI to select
those that will be uploaded to the cloud. Two steps are applied:
    1. Get rid of entire sessions which had corrupted or missing files (DTI
    and resting state).
    2. Retrieve only images from 3 modalities: anatomical, diffusion and fMRI
    (resting state).

@author: Eduardo Castro
"""

import os
import fnmatch
import numpy as np
import pandas as pd
from os.path import join as opj


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


# Load faulty imaging files
nifti_dir = '/data2/eduardo/datasets/PPMI/nifti'
faulty_dir = opj(nifti_dir, 'imaging_info')
with open(opj(faulty_dir, 'faulty_files.txt'), 'r') as fh:
    bad_fnames = fh.readlines()
    fh.close()

# Retrieve all filenames from PPMI
all_fnames = retrieve_img_fnames(nifti_dir, '*.nii')
np.save(opj(faulty_dir, 'all_img_fnames.npy'), all_fnames)

# Retrieve unique scan types (kind of, lots of redundancy)
scan_types = list(map(lambda x: x.split('/')[7], all_fnames))
scan_types = list(np.unique(scan_types))
scan_series = pd.Series(data=scan_types)
scan_series.to_csv(opj(faulty_dir, 'scan_types.csv'))

# Build a dataframe with all fnames: cols for subjid and modality
fnames_df = pd.DataFrame(data=all_fnames, columns=['fname'])
fnames_df['subjid'] = fnames_df['fname'].apply(lambda x: x.split('/')[6])
fnames_df['modality'] = fnames_df['fname'].apply(lambda x: x.split('/')[7])
fnames_df['date'] = fnames_df['fname'].apply(lambda x: x.split('/')[8])
fnames_df['series'] = fnames_df['fname'].apply(lambda x: x.split('/')[9])
fnames_df = fnames_df[['subjid', 'modality', 'date', 'series', 'fname']]

# Generate another one with faulty files
faulty_df = pd.DataFrame(data=bad_fnames, columns=['fname'])
faulty_df['subjid'] = faulty_df['fname'].apply(lambda x: x.split('/')[1])
faulty_df['modality'] = faulty_df['fname'].apply(lambda x: x.split('/')[2])
faulty_df['date'] = faulty_df['fname'].apply(lambda x: x.split('/')[3])
faulty_df['series'] = faulty_df['fname'].apply(lambda x: x.split('/')[4])
faulty_df = faulty_df[['subjid', 'modality', 'date', 'series', 'fname']]
del faulty_df['fname']
faulty_df = faulty_df.drop_duplicates()
faulty_df.reset_index(inplace=True, drop=True)

# If missing files for (subj, mod, date, scan series), then discard whole scan
relev_cols = ['subjid', 'modality', 'date', 'series']
for faulty_idx in faulty_df.index:
    faulty_tuple = list(faulty_df.loc[faulty_idx].values)
    fnames_idx = fnames_df.index[
            fnames_df[relev_cols].isin(faulty_tuple).all(axis=1).tolist()]
    if not fnames_idx.empty:
        fnames_df.drop(fnames_idx, inplace=True)
fnames_df.to_csv(opj(faulty_dir, 'all_ok_img_fnames.csv'), index=False)

# Find those with keywords (T1, MPRAGE, FSPGR, DTI, Rest, rs)
fnames_df = fnames_df[fnames_df['modality']
                      .str.contains('MPRAGE|FSPGR|DTI|Rest|rsfMRI',
                                    case=False, regex=True)]

# Discard funky ones (e.g., T1 & ax; DTI & FA, etc.)
fnames_df = fnames_df[~(fnames_df['modality'].str.contains('DTI', case=False)
                      & fnames_df['modality'].str.contains('FA(?!T)',
                                                           case=False,
                                                           regex=True))]
fnames_df = fnames_df[~(fnames_df['modality'].str.contains('FSPGR',
                        case=False) & fnames_df['modality'].str.contains(
                                'AX', case=False))]
fnames_df = fnames_df[~(fnames_df['modality'].str.contains('MPRAGE',
                        case=False) & fnames_df['modality'].str.contains(
                                'AX', case=False))]
fnames_df.to_csv(opj(faulty_dir, 'all_clear_img_fnames_cloud-db.csv'),
                 index=False)
