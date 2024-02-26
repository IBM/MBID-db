from botocore.client import Config
import ibm_boto3
import os
import sys
import pdb
from pathlib import Path
from shutil import copyfile
from tqdm import tqdm
import gc
from sqlalchemy import update
import ants as an
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Image, FileType, PreprocessTask, PreprocessTaskFile, SourceDataset
from config.globals import ENVIRONMENT
from preproc_t1_tools_alt import run_preproc
import os
from os.path import exists
import glob
from multiprocessing import Pool, Manager
from os import cpu_count
from flask import g
import shutil
import argparse

os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'




parser = argparse.ArgumentParser(description='Manually set the PID to block the images to be proccesed')
parser.add_argument('--pid', dest='pid',type=int,
                    help='process id')

template_file_path = Path('__file__').parent / 'template_file/MNI152_T1_1mm_brain.nii.gz'
mask_template_file_path = Path('__file__').parent / 'template_file/MNI152_T1_1mm_brain_mask.nii.gz'

def create_update_preprocess_task_file(t1_image, file_type, existing_process_task, preprocess_file_path, pearson_r=None):
	existing_process_task_file = PreprocessTaskFile.query.filter(
		PreprocessTaskFile.image_id == t1_image.id,
		PreprocessTaskFile.file_type_id == file_type.id,
		PreprocessTaskFile.preprocess_task_id == existing_process_task.id).first()
	if existing_process_task_file:
		existing_process_task_file.preprocess_file_path = preprocess_file_path
		if pearson_r:
			check_json = {'r':pearson_r}
			existing_process_task_file.preprocess_check_json = check_json
		db.session.merge(existing_process_task_file)
	else:
		if pearson_r:
			check_json = {'r': pearson_r}
			existing_process_task_file = PreprocessTaskFile(image_id=t1_image.id,
															file_type_id=file_type.id,
															preprocess_task_id=existing_process_task.id,
															preprocess_file_path=preprocess_file_path,
															preprocess_check_json=check_json)
		else:
			existing_process_task_file = PreprocessTaskFile(image_id=t1_image.id,
																  file_type_id=file_type.id,
																  preprocess_task_id=existing_process_task.id,
																  preprocess_file_path=preprocess_file_path)
		db.session.add(existing_process_task_file)
	db.session.commit()

def process_image(t1_image):
	import tensorflow as tf
	tf.config.set_visible_devices([], 'GPU')
	temp_file_path = Path('__file__').parent / '../temp/temp_preprocessing_{0}.nii.gz'.format(t1_image.id)
	if not exists(temp_file_path):
		img_mask_r = 'failed'
		return t1_image, img_mask_r
	temp_file_size = os.path.getsize(temp_file_path)
	# 5KB
	if temp_file_size<5000:
		img_mask_r = 'failed'
		return t1_image, img_mask_r
	try:
		registered_img, transform, mask_img, img_mask_r = run_preproc(temp_file_path, template_file_path,
																	  mask_template_file_path)
		registered_img.to_filename('../temp/temp_registered_{0}.nii.gz'.format(t1_image.id))
		copyfile(transform[0], '../temp/transform_{0}.mat'.format(t1_image.id))
		mask_img.to_file('../temp/mask_img_{0}.nii.gz'.format(t1_image.id))
	except:
		img_mask_r = 'failed'
	return t1_image, img_mask_r

if __name__ == '__main__':
	config = os.environ
	app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
	num_workers = int(0.8 * cpu_count())
	with app.app_context():
		args = parser.parse_args()
		total_images_to_preprocess = [True]
		while len(total_images_to_preprocess)>0:
			total_images_to_preprocess = Image.query.filter(Image.type == 'T1', Image.preprocessed.is_(None), Image.blocking_processing_id.is_(None)).all()
			images_per_worker = min(10000,len(total_images_to_preprocess))
			# Process id that started blocking
			if args.pid:
				process_id = args.pid
			else:
				process_id = os.getpid()
			# Update images to block them in db
			t1_images_to_block = Image.query.filter(Image.type == 'T1', Image.preprocessed.is_(None), Image.blocking_processing_id.is_(None)).limit(images_per_worker).all()
			for t1_block in t1_images_to_block:
				t1_block.preprocessed = 'locked'
				t1_block.blocking_processing_id = process_id
				db.session.merge(t1_block)
			db.session.commit()
			t1_images = [True]
			images_to_process_per_batch = max(10, num_workers)
			cos_client = ibm_boto3.client('s3',
										  ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
										  ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
										  ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
										  config=Config(signature_version='oauth'),
										  endpoint_url=app.config['SERVICE_ENDPOINT'])
			cos_client_ukbb = ibm_boto3.client('s3',
										  ibm_api_key_id=app.config['COS_CREDENTIALS_UKBB']['apikey'],
										  ibm_service_instance_id=app.config['COS_CREDENTIALS_UKBB']['resource_instance_id'],
										  ibm_auth_endpoint=app.config['AUTH_ENDPOINT_UKBB'],
										  config=Config(signature_version='oauth'),
										  endpoint_url=app.config['SERVICE_ENDPOINT_UKBB'])
			while len(t1_images)>0:
				# Get images blocked for processing
				t1_images = Image.query.join(SourceDataset.image).filter(Image.type == 'T1', Image.preprocessed=='locked', Image.blocking_processing_id == process_id).limit(images_to_process_per_batch).all()
				process_task_description = 'T1 minimal1: un-bias, skull strip, rigid and scale to MNI'
				existing_process_task = PreprocessTask.query.filter(
					PreprocessTask.description == process_task_description).first()
				image_file_type = FileType.query.filter(FileType.designation == 'T1w_2_MNI_rigid_w_scale_image').first()
				transformation_file_type = FileType.query.filter(
					FileType.designation == 'T1w_2_MNI_rigid_w_scale_affine').first()
				mask_file_type = FileType.query.filter(
					FileType.designation == 'brain_mask_T1w_native_space').first()
				if existing_process_task:
					existing_process_task.description = process_task_description
					db.session.merge(existing_process_task)
				else:
					existing_process_task = PreprocessTask(description=process_task_description)
					db.session.add(existing_process_task)
				db.session.commit()

				for t1_image in tqdm(t1_images):
					temp_file_path = Path('__file__').parent / '../temp/temp_preprocessing_{0}.nii.gz'.format(t1_image.id)
					if t1_image.source_dataset.designation == 'UK Biobank':
						image_bucket = app.config['BUCKET_UKBB']
						cos_client_download = cos_client_ukbb
					else:
						image_bucket = app.config['BUCKET']
						cos_client_download = cos_client
					with open(temp_file_path, 'wb') as f:
						cos_client_download.download_fileobj(image_bucket, t1_image.image_path, f)
				chunksize = max(1, int(len(t1_images) / num_workers))
				with Pool(num_workers) as p:
					#try function partial
					matrix = p.map(process_image, t1_images, chunksize)
				for t1_image_processed, img_mask_r in matrix:
					if img_mask_r == 'failed':
						t1_image_processed.preprocessed = 'failed'
						db.session.merge(t1_image_processed)
						db.session.commit()
					else:
						if t1_image_processed.source_dataset.designation == 'UK Biobank':
							image_bucket_upload = app.config['BUCKET_UKBB']
							cos_client_upload = cos_client_ukbb
						else:
							image_bucket_upload = app.config['BUCKET']
							cos_client_upload = cos_client
						image_file_path = "preprocess_files/T1_minimal1/"+str(t1_image_processed.id)+"_" + t1_image_processed.image_path.split('/')[-1].replace(
							'.nii.gz',
							'') + '_2_MNI_rigid_w_scale.nii.gz'
						transformation_file_path = "preprocess_files/T1_minimal1/" +str(t1_image_processed.id)+"_" + t1_image_processed.image_path.split('/')[-1].replace(
							'.nii.gz',
							'') + '_2_MNI_rigid_w_scale.mat'
						mask_file_path = "preprocess_files/T1_minimal1/" +str(t1_image_processed.id)+"_" + t1_image_processed.image_path.split('/')[-1].replace('.nii.gz',
																													  '') + '_mask_native.nii.gz'
						temp_registered_path = '../temp/temp_registered_{0}.nii.gz'.format(t1_image_processed.id)
						temp_transform_path = '../temp/transform_{0}.mat'.format(t1_image_processed.id)
						temp_mask_img_path = '../temp/mask_img_{0}.nii.gz'.format(t1_image_processed.id)
						if exists(temp_registered_path) and exists(temp_transform_path) and exists(temp_mask_img_path):
							cos_client_upload.upload_file(temp_registered_path, image_bucket_upload,
												   image_file_path)
							cos_client_upload.upload_file(temp_transform_path, image_bucket_upload,
												   transformation_file_path)
							cos_client_upload.upload_file(temp_mask_img_path, image_bucket_upload,
												   mask_file_path)
							create_update_preprocess_task_file(t1_image_processed, image_file_type, existing_process_task, image_file_path,
															   img_mask_r)
							create_update_preprocess_task_file(t1_image_processed, transformation_file_type, existing_process_task,
															   transformation_file_path, img_mask_r)
							create_update_preprocess_task_file(t1_image_processed, mask_file_type, existing_process_task,
															   mask_file_path, img_mask_r)
							t1_image_processed.preprocessed = 'minimal1'
							t1_image_processed.blocking_processing_id = None
							db.session.merge(t1_image_processed)
							db.session.commit()
						else:
							t1_image_processed.preprocessed = 'failed'
							db.session.merge(t1_image_processed)
							db.session.commit()
						os.remove(temp_registered_path)
						os.remove(temp_transform_path)
						os.remove(temp_mask_img_path)
				for t1_image in t1_images:
					temp_file_path = Path('__file__').parent / '../temp/temp_preprocessing_{0}.nii.gz'.format(
						t1_image.id)
					if exists(temp_file_path):
						os.remove(temp_file_path)

				print('{0} images were processed'.format(images_to_process_per_batch))
