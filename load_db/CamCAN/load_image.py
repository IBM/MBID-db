from botocore.client import Config
import ibm_boto3
import re
import os
import sys
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner, Condition
from load_file.utils import read_s3_contents

if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'Cam-CAN').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'Cam-CAN/cc700/mri'

        for i in bucket.objects.filter(Prefix=prefix):
            if (i.key.endswith("nii.gz")):
                json_filename = i.key.replace(".nii.gz",".json")
                # Scanner information. Reading json file for this
                try:
                    mri_metadata = read_s3_contents(cos_client, app.config['BUCKET'],
                                                        json_filename)
                    metadata = json.loads(mri_metadata.decode("utf-8"))
                    existing_scanner = Scanner.query.filter(Scanner.brand == metadata["Manufacturer"],
                                                            Scanner.model == metadata["ManufacturersModelName"],
                                                            Scanner.source_id == metadata["DeviceSerialNumber"],
                                                            Scanner.source_dataset_id == dataset_object.id).first()
                    scanner_teslas = None
                    if metadata['MagneticFieldStrength'] in ['3', 3]:
                        scanner_teslas = 'three'
                    elif metadata['MagneticFieldStrength'] in ['1.5', 1.5]:
                        scanner_teslas = 'one_and_a_half'
                    else:
                        print(f"There is a scanner with different teslas: {metadata['MagneticFieldStrength']}")
                    if existing_scanner:
                        existing_scanner.teslas = scanner_teslas
                        db.session.merge(existing_scanner)
                    else:
                        existing_scanner = Scanner(brand = metadata['Manufacturer'],
                                                   model = metadata['ManufacturersModelName'],
                                                   source_id = metadata["DeviceSerialNumber"],
                                                   source_dataset_id=dataset_object.id, teslas = scanner_teslas)
                        db.session.add(existing_scanner)
                    db.session.commit()
                except cos_client.meta.client.exceptions.NoSuchKey:
                    print(f"Can't find image metadata for file {i.key}")
                    continue
                # Cam-CAN Dataset has several types of MRI for every patient, T1 and rsfMRI
                # T1 (anat), T2 (anat) , DWI (dwi) and MTI
            	# Functional: epi_rest
                # Fieldmaps	resting-state (fmap_rest), movie-watching (fmap_movie), and sensorimotor task (fmap_smt) 
                if ("T1w" in i.key):
                    image_type = 'T1'
                elif "epi_rest" in i.key:
                    image_type = 'rsfMRI'
                elif "dwi" in i.key:
                    image_type = 'DWI'
                    # Ensure that DWI images has bvec and bval data
                    try:
                        dwi_aditional_data = read_s3_contents(cos_client, app.config['BUCKET'],
                                                        i.key.replace(".nii.gz",".bvec"))
                        dwi_aditional_data = read_s3_contents(cos_client, app.config['BUCKET'],
                                                        i.key.replace(".nii.gz",".bval"))
                    except cos_client.meta.client.exceptions.NoSuchKey:
                        print(f"ERROR: The DWI image {i.key} has no bvec or bval data associated")
                        continue
                elif ("fmap_movie" in i.key or "fmap_smt" in i.key or "fmap_rest" in i.key) and "run" not in i.key:
                    image_type = 'FieldMap'
                # Skip images types that are not supported, in this case T2w and fieldmap with names run01 or run02
                elif "T2w" in i.key or "run" in i.key:
                    continue
                else:
                    print(f"ERROR: Unable to select image type from file: {i.key}")
                    continue
                # CamCAN Dataset is all about age, so there are no symptoms, conditions or bmi information
                condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
                days_since_baseline = 0
                try:
                    # Extract subject external id from the name of the file
                    subject_external_id = re.search(re.compile('sub-CC\d{6}'), i.key).group(0).replace("sub-", "")
                except:
                    print(f"ERROR: Unable to extract external id from file: {i.key}")
                    continue

                # Visit parameters: CamCAN has no visit, os adding sald_visit as external id
                visit_external_id = '1'
                visit_days_since_baseline = 0
                existing_subject = Subject.query.filter(Subject.external_id == subject_external_id,
                                                        Subject.source_dataset_id == dataset_object.id).first()
                if existing_subject:
                    existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                        Visit.subject_id == existing_subject.id,
                                                        Visit.source_dataset_id == dataset_object.id).first()

                    if existing_visit:
                        existing_visit.condition_id = condition.id
                        db.session.merge(existing_visit)
                    else:
                        existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                                source_dataset_id=dataset_object.id,
                                                days_since_baseline=visit_days_since_baseline,
                                                condition_id = condition.id)
                        db.session.add(existing_visit)
                    db.session.commit()

                    ###########################################################
                    # Image info
                    ###########################################################
                    existing_image = Image.query.filter(Image.visit_id == existing_visit.id,
                                                        Image.subject_id == existing_subject.id,
                                                        Image.source_dataset_id == dataset_object.id,
                                                        Image.image_path == i.key).first()
                    if existing_image:
                        existing_image.image_path = i.key
                        existing_image.file_size = i.size
                        existing_image.type = image_type
                        existing_image.days_since_baseline = visit_days_since_baseline
                        existing_image.metadata_json = metadata
                        existing_image.scanner_id = existing_scanner.id
                        db.session.merge(existing_image)
                    else:
                        image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                            source_dataset_id=dataset_object.id,
                                            image_path=i.key, file_size=i.size,
                                            type=image_type, metadata_json=metadata,
                                            days_since_baseline=visit_days_since_baseline,
                                            scanner_id=existing_scanner.id)
                        db.session.add(image_db)
                    db.session.commit()
                else:
                    print('ERROR: missing subject id ' + subject_external_id)
                    continue
