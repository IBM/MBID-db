import os
os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = '1'
os.environ['TF_NUM_INTEROP_THREADS'] = '1'
os.environ['TF_NUM_INTRAOP_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'

from botocore.client import Config
import ibm_boto3
import sys
from pathlib import Path
from shutil import copyfile
from tqdm import tqdm
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             '..', '..')))
from app import create_app, db
from app.models import Image, FileType, PreprocessTask, PreprocessTaskFile,\
                       SourceDataset
from config.globals import ENVIRONMENT
from preproc_t1_tools_nonrig import run_preproc
import os
import glob
from os.path import exists
from multiprocessing import Pool
from os import cpu_count
import argparse


"""
FUNCTIONS
"""

def create_update_preprocess_task_file(t1_image, file_type,
                                       existing_process_task,
                                       preprocess_file_path, pearson_r=None):
    """
    Create (or update if already existent) an entry on the
    PreprocessTaskFile table of the database after having run the
    preprocessing for it.

    Parameters
    ----------
    t1_image : Entry from Image table
        T1W entry from the SQL image table in COS.
    file_type : Entry from FileType table 
        Defines the type of entry in PreprocessTaskFile, e.g.,
        rigid-registered brain, rigid transform, native brain mask, etc.
    existing_process_task : Entry from PreprocessTask
        Specifies the type of registration associated to this file.
    preprocess_file_path : str
        Location in COS of this file.
    pearson_r : float, optional
        Correlation between the registered and the template brain.
        The default is None.

    Returns
    -------
    None.
    """
    
    existing_process_task_file = PreprocessTaskFile.query.filter(
        PreprocessTaskFile.image_id == t1_image.id,
        PreprocessTaskFile.file_type_id == file_type.id,
        PreprocessTaskFile.preprocess_task_id == existing_process_task.id).first()
    if existing_process_task_file:
        existing_process_task_file.preprocess_file_path = preprocess_file_path
        if pearson_r:
            check_json = {'r': pearson_r}
            existing_process_task_file.preprocess_check_json = check_json
        db.session.merge(existing_process_task_file)
    else:
        if pearson_r:
            check_json = {'r': pearson_r}
            existing_process_task_file =\
                PreprocessTaskFile(image_id=t1_image.id,
                                   file_type_id=file_type.id,
                                   preprocess_task_id=existing_process_task.id,
                                   preprocess_file_path=preprocess_file_path,
                                   preprocess_check_json=check_json)
        else:
            existing_process_task_file =\
                PreprocessTaskFile(image_id=t1_image.id,
                                   file_type_id=file_type.id,
                                   preprocess_task_id=existing_process_task.id,
                                   preprocess_file_path=preprocess_file_path)
        db.session.add(existing_process_task_file)
    db.session.commit()


def process_image(image_dict):
    """
    Run preprocessing for a given T1W image

    Parameters
    ----------
    image_dict : dict
        Dictionary with 2 fields: an Image table entry with the T1 weighted
        image to preprocess, and a list of strings to be used for the initial
        transform prior to the nonlinear transformation (or None if not
        applicable)

    Returns
    -------
    t1_image : Entry from Image Table
        T1W entry from the SQL image table in the DB.
    img_mask_r : int
        Correlation value between the registered and the template brain.
    """
    import tensorflow as tf
    tf.config.set_visible_devices([], 'GPU')
    
    # Retrieve T1W Image table entry and list of init transforms
    t1_image = image_dict['t1_image']
    init_transf = image_dict['init_transf']
    if init_transf is None:
        reg_type = 'rigid'
    else:
        reg_type = 'nonlinear'
    
    # Define input and target filenames to be used for registration
    template_file_path =\
        Path('__file__').parent / 'template_file/MNI152_T1_1mm_brain.nii.gz'
    mask_template_file_path =\
        Path('__file__').parent /\
            'template_file/MNI152_T1_1mm_brain_mask.nii.gz'
    
    temp_file_path =\
        (Path('__file__').parent / \
            f'../temp/temp_preprocessing_{t1_image.id}.nii.gz').resolve()
            
    # Define directory (and file prefix) to dump ANTs temporary files
    temp_dump_path = (Path('__file__').parent / '../temp/ants_dump').resolve()\
        / str(t1_image.id)
    temp_dump_path = str(temp_dump_path)
    
    # Corrupt input: if imaging file not existent or <5KB, then it failed
    if not exists(temp_file_path):
        img_mask_r = 'failed'
        return t1_image, img_mask_r
    
    # Also, report failure if it's smaller than 5KB
    temp_file_size = os.path.getsize(temp_file_path)
    if temp_file_size < 5000:
        img_mask_r = 'failed'
        return t1_image, img_mask_r
    try:
        registered_img, transform, mask_img, img_mask_r =\
            run_preproc(temp_file_path, template_file_path,
                        mask_template_file_path, init_transf,
                        reg_type, out_prefix=temp_dump_path)
        registered_img.to_filename(f'../temp/temp_registered_{t1_image.id}.nii.gz')
        if reg_type == 'rigid':
            copyfile(transform[0], f'../temp/transform_{t1_image.id}.mat')
            mask_img.to_file(f'../temp/mask_img_{t1_image.id}.nii.gz')
        else:
            copyfile(transform[0], f'../temp/transform_{t1_image.id}.nii.gz')
    except:
        img_mask_r = 'failed'
    return t1_image, img_mask_r


"""
MAIN FUNCTION
"""

if __name__ == '__main__':
    # Define fraction of CPUs to use
    cpu_frac = 0.5
 
    # Set up parser
    parser =\
        argparse.ArgumentParser(description='Script to run the preprocessing '
                                'pipeline and update entries of the SQL '
                                'imaging database.')
    parser.add_argument('--reg_type', dest='reg_type', type=str,
                        help=('Type of preprocessing. Valid values are '
                              '"rigid" and "nonlinear"'))
    parser.add_argument('--pid', dest='pid', type=int,
                        help='Process id')
    parser.add_argument('--study', dest='study', type=str,
                        help='Study to be processed')
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    # Define dictionary for images' preprocessing stage (DB)
    pproc_dict = {'rigid': None, 'nonlinear': 'minimal1'}
    
    # Retrieve parser vars, specify task description and relevant filenames
    args = parser.parse_args()
    study = args.study

    mask_fn = 'brain_mask_T1w_native_space'
    mask_suff = 'mask_native.nii.gz'
    if args.reg_type == 'rigid':
        process_task_description = ('T1 minimal1: un-bias, '
                                    'skull strip, rigid and scale to MNI')
        proc_type = 'minimal1'
        pproc_dir = 'T1_' + proc_type
        
        img_fn = 'T1w_2_MNI_rigid_w_scale_image'
        trans_fn = 'T1w_2_MNI_rigid_w_scale_affine'
        init_transf_patt = None
        pproc_img_suff = '2_MNI_rigid_w_scale.nii.gz'
        trans_suff = '2_MNI_rigid_w_scale.mat'
    else:
        process_task_description = ('T1 warping: nonlinear registration '
                                    'to MNI after T1 minimal1')
        proc_type = 'warping'
        pproc_dir = 'T1_' + proc_type
        
        img_fn = 'T1w_2_MNI_warped_image'
        trans_fn = 'T1w_2_MNI_warp_grid'
        init_transf_patt = 'T1w_2_MNI_rigid_w_scale_affine'
        pproc_img_suff = '2_MNI_warped_image.nii.gz'
        trans_suff = '2_MNI_warp_grid.nii.gz'

    # API setup; iterate through images
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    num_workers = int(cpu_frac * cpu_count())
    
    with app.app_context():
        assert args.reg_type in ['rigid', 'nonlinear'],\
            'Valid values are only "rigid" and "nonlinear"'
        pproc_status = pproc_dict[args.reg_type]
        
        total_images_to_preprocess = [True]
        while len(total_images_to_preprocess) > 0:
            # If study is None, ignore study filtering
            if study is None:
                total_images_to_preprocess =\
                    Image.query.filter(Image.type == 'T1',
                                       Image.preprocessed == pproc_status,
                                       Image.blocking_processing_id.is_(None)).all()
            else:
                dataset_object =\
                    SourceDataset.query.filter(SourceDataset.designation
                                               == study).first()          
                total_images_to_preprocess =\
                    Image.query.filter(Image.type == 'T1',
                                       Image.preprocessed == pproc_status,
                                       Image.source_dataset_id == 
                                           dataset_object.id,
                                       Image.blocking_processing_id.is_(None)).all()
            images_per_worker = min(10000, len(total_images_to_preprocess))
            
            # Process id that started blocking
            if args.pid:
                process_id = args.pid
            else:
                process_id = os.getpid()
            
            # Update images to block them in db
            if study is None:
                t1_images_to_block =\
                    Image.query.filter(Image.type == 'T1',
                                       Image.preprocessed == pproc_status,
                                       Image.blocking_processing_id.is_(None))\
                        .limit(images_per_worker).all()
            else:
                t1_images_to_block =\
                    Image.query.filter(Image.type == 'T1',
                                       Image.preprocessed == pproc_status,
                                       Image.source_dataset_id == 
                                           dataset_object.id,
                                       Image.blocking_processing_id.is_(None))\
                        .limit(images_per_worker).all()

            for t1_block in t1_images_to_block:
                t1_block.preprocessed = 'locked'
                t1_block.blocking_processing_id = process_id
                db.session.merge(t1_block)
            db.session.commit()
            
            # Set up number of images to process; define COS to use (UKBB or rest)
            t1_images = [True]
            images_to_process_per_batch = max(10, num_workers)
            cos_client = ibm_boto3.client('s3',
                                          ibm_api_key_id=\
                                              app.config['COS_CREDENTIALS']\
                                                  ['apikey'],
                                          ibm_service_instance_id=\
                                              app.config['COS_CREDENTIALS']\
                                                  ['resource_instance_id'],
                                          ibm_auth_endpoint=\
                                              app.config['AUTH_ENDPOINT'],
                                          config=\
                                              Config(signature_version='oauth'),
                                          endpoint_url=\
                                              app.config['SERVICE_ENDPOINT'])
            cos_client_ukbb = ibm_boto3.client('s3',
                                          ibm_api_key_id=\
                                              app.config['COS_CREDENTIALS_UKBB']\
                                                  ['apikey'],
                                          ibm_service_instance_id=\
                                              app.config['COS_CREDENTIALS_UKBB']\
                                                  ['resource_instance_id'],
                                          ibm_auth_endpoint=\
                                              app.config['AUTH_ENDPOINT_UKBB'],
                                          config=\
                                              Config(signature_version='oauth'),
                                          endpoint_url=\
                                              app.config['SERVICE_ENDPOINT_UKBB'])
            
            while len(t1_images) > 0:
                # Get subset of images blocked for processing
                t1_images =\
                    Image.query.join(SourceDataset.image).\
                        filter(Image.type == 'T1',
                               Image.preprocessed=='locked',
                               Image.blocking_processing_id == process_id).\
                            limit(images_to_process_per_batch).all()
                                
                # Retrieve affine transform for each T1 (if applicable)
                if args.reg_type == 'rigid':
                    init_transf_list = [None] * len(t1_images)
                else:
                    init_transf_list = []
                    init_trans_type = FileType.query.filter(
                        FileType.designation == init_transf_patt).first()
                    for t1_img in tqdm(t1_images):
                        if t1_img.source_dataset.designation == 'UK Biobank':
                            image_bucket = app.config['BUCKET_UKBB']
                            cos_client_download = cos_client_ukbb
                        else:
                            image_bucket = app.config['BUCKET']
                            cos_client_download = cos_client
                        
                        process_task_file = PreprocessTaskFile.query.filter(
                            PreprocessTaskFile.image_id == t1_img.id,
                            PreprocessTaskFile.file_type_id ==\
                                init_trans_type.id).first()
                        pproc_cos_fn = process_task_file.preprocess_file_path
                        pproc_local_fn = f'../temp/transform_{t1_img.id}.mat'
                        
                        with open(pproc_local_fn, 'wb') as f:
                            cos_client_download.download_fileobj(image_bucket,
                                                                 pproc_cos_fn,
                                                                 f)
                        init_transf_list.append([pproc_local_fn])
                
                # Create dictionary with T1W images and associated affine transf
                image_dict_list =\
                    [dict(zip(['t1_image', 'init_transf'], vals))
                     for vals in zip(t1_images, init_transf_list)]
                
                # Retrieve associated processing entry and file types
                existing_process_task = PreprocessTask.query.filter(
                    PreprocessTask.description ==
                        process_task_description).first()
                image_file_type = FileType.query.filter(
                    FileType.designation == img_fn).first()
                transformation_file_type = FileType.query.filter(
                    FileType.designation == trans_fn).first()
                mask_file_type = FileType.query.filter(
                    FileType.designation == mask_fn).first()
                
                # If the no preprocessing entry, add it to ProcessTask table
                if not existing_process_task:
                    existing_process_task =\
                        PreprocessTask(description=process_task_description)
                    db.session.add(existing_process_task)
                db.session.commit()

                # Do the same for file types
                if not image_file_type:
                    image_file_type = FileType(designation=img_fn)
                    db.session.add(image_file_type)
                db.session.commit()
                
                if not transformation_file_type:
                    transformation_file_type = FileType(designation=trans_fn)
                    db.session.add(transformation_file_type)
                db.session.commit()
                
                if not mask_file_type:
                    mask_file_type =\
                        FileType(designation=mask_fn)
                    db.session.add(mask_file_type)
                db.session.commit()
                
                # Download T1W imaging files locally (temporary location)
                for t1_image in tqdm(t1_images):
                    temp_file_path = Path('__file__').parent /\
                        f'../temp/temp_preprocessing_{t1_image.id}.nii.gz'
                    if t1_image.source_dataset.designation == 'UK Biobank':
                        image_bucket = app.config['BUCKET_UKBB']
                        cos_client_download = cos_client_ukbb
                    else:
                        image_bucket = app.config['BUCKET']
                        cos_client_download = cos_client
                    with open(temp_file_path, 'wb') as f:
                        cos_client_download.download_fileobj(image_bucket,
                                                             t1_image.image_path,
                                                             f)
                                
                # Run processing in parallel
                chunksize = max(1, int(len(t1_images) / num_workers))
                with Pool(num_workers) as p:
                    results_pproc = p.map(process_image, image_dict_list,
                                          chunksize)
                
                # Upload preprocessed files (iterative) and update DB entries
                for t1_image_processed, img_mask_r in results_pproc:
                    if img_mask_r == 'failed':
                        t1_image_processed.preprocessed = 'failed'
                        db.session.merge(t1_image_processed)
                        db.session.commit()
                    else:
                        if (t1_image_processed.source_dataset.designation ==
                            'UK Biobank'):
                            image_bucket_upload = app.config['BUCKET_UKBB']
                            cos_client_upload = cos_client_ukbb
                        else:
                            image_bucket_upload = app.config['BUCKET']
                            cos_client_upload = cos_client
                        base_fn =\
                            t1_image_processed.image_path.split('/')[-1]
                        base_fn = base_fn.replace('.nii.gz', '')
                        image_file_path = (f'preprocess_files/{pproc_dir}/' +
                                           '_'.join([str(t1_image_processed.id),
                                                     base_fn, pproc_img_suff]))
                        transformation_file_path =\
                            (f'preprocess_files/{pproc_dir}/' +
                             '_'.join([str(t1_image_processed.id), base_fn,
                                       trans_suff]))
                        mask_file_path = ('preprocess_files/T1_minimal1/' +
                                          '_'.join([str(t1_image_processed.id),
                                                    base_fn, mask_suff]))
                        
                        temp_registered_path =\
                            f'../temp/temp_registered_{t1_image_processed.id}.nii.gz'
                        if args.reg_type == 'rigid':
                            temp_transform_path =\
                                f'../temp/transform_{t1_image_processed.id}.mat'
                            temp_mask_img_path =\
                                f'../temp/mask_img_{t1_image_processed.id}.nii.gz'
                            files_exist = exists(temp_registered_path)\
                                and exists(temp_transform_path)\
                                and exists(temp_mask_img_path)
                        else:
                            temp_transform_path =\
                                f'../temp/transform_{t1_image_processed.id}.nii.gz'
                            files_exist = exists(temp_registered_path)\
                                and exists(temp_transform_path)
                        
                        if files_exist:
                            cos_client_upload.upload_file(temp_registered_path,
                                                          image_bucket_upload,
                                                          image_file_path)
                            cos_client_upload.upload_file(temp_transform_path,
                                                          image_bucket_upload,
                                                          transformation_file_path)
                            if args.reg_type == 'rigid':
                                cos_client_upload.upload_file(temp_mask_img_path,
                                                              image_bucket_upload,
                                                              mask_file_path)
                            
                            create_update_preprocess_task_file(t1_image_processed,
                                                               image_file_type,
                                                               existing_process_task,
                                                               image_file_path,
                                                               img_mask_r)
                            create_update_preprocess_task_file(t1_image_processed,
                                                               transformation_file_type,
                                                               existing_process_task,
                                                               transformation_file_path,
                                                               img_mask_r)
                            if args.reg_type == 'rigid':
                                create_update_preprocess_task_file(t1_image_processed,
                                                                   mask_file_type,
                                                                   existing_process_task,
                                                                   mask_file_path,
                                                                   img_mask_r)
                            t1_image_processed.preprocessed = proc_type
                            t1_image_processed.blocking_processing_id = None
                            db.session.merge(t1_image_processed)
                            db.session.commit()
                        else:
                            t1_image_processed.preprocessed = 'failed'
                            db.session.merge(t1_image_processed)
                            db.session.commit()
                        os.remove(temp_registered_path)
                        os.remove(temp_transform_path)
                        if args.reg_type == 'rigid':
                            os.remove(temp_mask_img_path)
                
                # Remove temporary files from local server
                for t1_image in t1_images:
                    temp_file_path = Path('__file__').parent /\
                        f'../temp/temp_preprocessing_{t1_image.id}.nii.gz'
                    if exists(temp_file_path):
                        os.remove(temp_file_path)
                    
                    if args.reg_type == 'nonlinear':
                        temp_trans_path = Path('__file__').parent /\
                            f'../temp/transform_{t1_image.id}.mat'
                        if exists(temp_trans_path):
                            os.remove(temp_trans_path)
                
                # Remove temporary files generated by ANTs
                temp_files = (Path('__file__').parent /\
                              '../temp/ants_dump').resolve() / '*'
                temp_files = glob.glob(str(temp_files))
                for f in temp_files:
                    os.remove(f)
                
                print(f'{images_to_process_per_batch} images were processed')
