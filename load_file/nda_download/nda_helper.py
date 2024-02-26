import subprocess
from pathlib import Path
import numpy as np
import nibabel as nib
from multiprocessing import Pool
import pandas as pd
import re

def nda_s3_download_single_file(pkg_id, s3_path, out_dir, username, passwd):
    cmd = (f'downloadcmd --package {pkg_id} {s3_path} -u {username} '
           f'-p {passwd} -d {out_dir} -wt 1')
    op = subprocess.check_output(cmd, shell=True)
    print(op)
    return


def nda_s3_download_file_list(pkg_id, s3_path_list, out_dir, username, passwd,
                              threads=8):
    temp_txt = Path(out_dir) / 'flist_{}.txt'.format(int(np.random.rand()*1e6))
    with open(temp_txt, 'w') as fl:
        fl.writelines(ln + '\n' for ln in s3_path_list)
    cmd = (f'downloadcmd --package {pkg_id} -t {temp_txt} -u {username} '
           f'-p {passwd} -d {out_dir} -wt {threads}')
    subprocess.run(cmd, shell=True, check=True)
    # op = subprocess.check_output(cmd, shell=True)
    # print(op.decode('UTF-8'))
    temp_txt.unlink()
    out_fns = [(out_dir / 'imagingcollection01' /
               '/'.join(fn.split('/')[4:])) for fn in s3_path_list]
    fn_exists = np.array([fn.exists() for fn in out_fns])
    return out_fns, fn_exists


def merge_4d_niftis(nii_fns, out_fn):
    arrays = []
    affines = []
    for ix, fn in enumerate(nii_fns):
        img = nib.load(fn)
        arrays.append(img.get_fdata())
        affines.append(img.affine)
        if ix == 0:
            header = img.header
    # make sure the affine matrix of all images are the same:
    assert np.all(np.isclose(np.sum(affines, axis=0), affines[0]*len(affines),
                             atol=1e-4))
    out_arr = np.concatenate(arrays, axis=-1)
    out_img = nib.Nifti1Image(out_arr, affines[0], header=header)
    nib.save(out_img, out_fn)
    return


def merge_dwi_bvec(bvec_fns, out_fn):
    vecs = []
    for fn in bvec_fns:
        bvec = np.loadtxt(fn)
        vecs.append(bvec)
    out_vec = np.concatenate(vecs, axis=1)
    np.savetxt(out_fn, out_vec, fmt=str('%.14f'))
    return


def merge_dwi_bvals(bval_fns, out_fn):
    vecs = []
    for fn in bval_fns:
        bval = np.loadtxt(fn)
        vecs.append(bval)
    out_vec = np.concatenate(vecs)[None, :]
    np.savetxt(out_fn, out_vec, fmt=str('%.6f'))
    return


def copy_first_file(in_fns, out_fn):
    src = Path(in_fns[0])
    dest = Path(out_fn)
    dest.write_bytes(src.read_bytes())
    return
    
def hca_dwi_merge_from_full_df(full_df, subject_list, n_processors):

    # find rows of all DWI-related files:
    is_dwi = full_df['modality'] == 'DWI'
    is_nii = full_df['local_filename'].apply(lambda x: x.endswith('.nii.gz'))
    is_json = full_df['local_filename'].apply(lambda x: x.endswith('.json'))
    is_bval = full_df['local_filename'].apply(lambda x: x.endswith('.bval'))
    is_bvec = full_df['local_filename'].apply(lambda x: x.endswith('.bvec'))
    is_AP = full_df['local_filename'].apply(lambda x: x.find('_AP') != -1)
    is_PA = full_df['local_filename'].apply(lambda x: x.find('_PA') != -1)

    phase_dic = {'AP': is_AP, 'PA': is_PA}
    ftype_dic = {'nii': is_nii, 'bval': is_bval, 'bvec': is_bvec,
                 'json': is_json}
    merge_fun_dic = {'nii': merge_4d_niftis, 'bval': merge_dwi_bvals,
                     'bvec': merge_dwi_bvec, 'json': copy_first_file}

    # For each subject and file type (nii, bval, bvec) make list of input file
    # pairs and output file names:
    merge_param_dic = {}
    for ftype, ftype_rows in ftype_dic.items():
        input_pairs = []
        merged_outputs = []
        merged_subjs = []
        for subjid in subject_list:
            subj_rows = full_df['subjid'] == subjid
            for phase_name, phase_rows in phase_dic.items():
                frows = is_dwi & ftype_rows & phase_rows & subj_rows
                used_in_fns = np.sort(full_df[frows]['local_filename'].values)
                if len(used_in_fns) == 2:
                    path_parts = used_in_fns[0].split('/')
                    stem = path_parts[-1]
                    out_stem = re.sub(r'_dir9[98]', '_merged', stem)
                    out_fn = '/'.join(path_parts[:-1] + [out_stem])
                    input_pairs.append(used_in_fns)
                    merged_outputs.append(out_fn)
                    merged_subjs.append(subjid)
        merge_param_dic[ftype] = (input_pairs, merged_outputs, merged_subjs)
        # Now execute the merge, for all subjects is subject_list:
        merge_args = list(zip(input_pairs, merged_outputs))
        with Pool(n_processors) as p:
            p.starmap(merge_fun_dic[ftype], merge_args)
        # merge_4d_niftis(nii_fn, out_fn)

    # Now, these output results will need to be appended to the full_df:
    df_list = [full_df]
    for ftype, merge_tuple in merge_param_dic.items():
        per_type_arr = np.c_[merge_tuple[2],  # subjids
                             np.full((len(merge_tuple[2]),), ''),  # s3 path
                             np.full((len(merge_tuple[2]),), 'DWI'),  # mod
                             merge_tuple[1]]  # local_filename
        per_type_df = pd.DataFrame(per_type_arr, columns=full_df.columns)
        df_list.append(per_type_df)
    full_df = pd.concat(df_list, ignore_index=True)
    return full_df


def hca_rsfmri_merge_from_full_df(full_df, subject_list, n_processors):

    # find rows of all rsfMRI-related files:
    is_rsfmri = full_df['modality'] == 'rsfMRI'
    is_nii = full_df['local_filename'].apply(lambda x: x.endswith('.nii.gz'))
    is_json = full_df['local_filename'].apply(lambda x: x.endswith('.json'))
    is_AP = full_df['local_filename'].apply(lambda x: x.find('_AP') != -1)
    is_PA = full_df['local_filename'].apply(lambda x: x.find('_PA') != -1)

    phase_dic = {'AP': is_AP, 'PA': is_PA}
    ftype_dic = {'nii': is_nii, 'json': is_json}
    merge_fun_dic = {'nii': merge_4d_niftis, 'json': copy_first_file}

    # For each subject and file type (nii, json) make list of input file
    # pairs and output file names:
    merge_param_dic = {}
    for ftype, ftype_rows in ftype_dic.items():
        input_pairs = []
        merged_outputs = []
        merged_subjs = []
        for subjid in subject_list:
            subj_rows = full_df['subjid'] == subjid
            for phase_name, phase_rows in phase_dic.items():
                frows = is_rsfmri & ftype_rows & phase_rows & subj_rows
                used_in_fns = np.sort(full_df[frows]['local_filename'].values)
                if len(used_in_fns) == 2:
                    path_parts = used_in_fns[0].split('/')
                    stem = path_parts[-1]
                    out_stem = re.sub(r'_REST[12]', '_RESTmerged', stem)
                    out_fn = '/'.join(path_parts[:-1] + [out_stem])
                    input_pairs.append(used_in_fns)
                    merged_outputs.append(out_fn)
                    merged_subjs.append(subjid)
        merge_param_dic[ftype] = (input_pairs, merged_outputs, merged_subjs)
        # Now execute the merge, for all subjects is subject_list:
        merge_args = list(zip(input_pairs, merged_outputs))
        with Pool(n_processors) as p:
            p.starmap(merge_fun_dic[ftype], merge_args)
        # merge_4d_niftis(nii_fn, out_fn)

    # Now, these output results will need to be appended to the full_df:
    df_list = [full_df]
    for ftype, merge_tuple in merge_param_dic.items():
        per_type_arr = np.c_[merge_tuple[2],  # subjids
                             np.full((len(merge_tuple[2]),), ''),  # s3 path
                             np.full((len(merge_tuple[2]),), 'rsfMRI'),  # mod
                             merge_tuple[1]]  # local_filename
        per_type_df = pd.DataFrame(per_type_arr, columns=full_df.columns)
        df_list.append(per_type_df)
    full_df = pd.concat(df_list, ignore_index=True)
    return full_df
