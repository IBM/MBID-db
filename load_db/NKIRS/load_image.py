from botocore.client import Config
import ibm_boto3
import re
import os
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from load_db.NKIRS import get_baseline_visit, open_subject_metadata_file
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner, Condition
from load_file.utils import read_s3_contents

def calculate_days_since_baseline(subject_info_df, visit_external_id):
    """
    Calculates the visit's days since baseline using the subject session csv
    """
    baseline_visit = get_baseline_visit(subject_info_df)
    # In the CSV file, sometimes there are more than one entry per Visit.
    # In that case, getting just the first one
    try: 
        visit_days_since_enrollment = subject_info_df.loc[visit_external_id]['days_since_enrollment'].values[0]
    except AttributeError:
        try:
            visit_days_since_enrollment = subject_info_df.loc[visit_external_id]['days_since_enrollment']
        except:
            print(f'No available age at baseline: {subject_info_df}')
            return None
    except KeyError:
        print(f"No baseline visit {subject_info_df}")
        return None
    try: 
        baseline_days_since_enrollment = subject_info_df.loc[baseline_visit]['days_since_enrollment'].values[0]
    except AttributeError:
        try:
            baseline_days_since_enrollment = subject_info_df.loc[baseline_visit]['days_since_enrollment']
        except:
            print(f'No available age at baseline: {subject_info_df}')
            return None
    except KeyError:
        print(f"No baseline visit {subject_info_df}")
        return None
    return int(visit_days_since_enrollment - baseline_days_since_enrollment)


if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'NKIRS').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'NKIRS'
        avoid = ['sub-A00028995', 'sub-A00051548', 'sub-A00037229', 'sub-A00039277', 'sub-A00056099', 'sub-A00056295',
        	'sub-A00057726', 'sub-A00061413', 'sub-A00066248', 'sub-A00074447', 'sub-A00075292', 'sub-A00082665', 'sub-A00085866']
        for i in bucket.objects.filter(Prefix=prefix):
            if (i.key.endswith("nii.gz")):
                json_filename = i.key.replace(".nii.gz",".json")
                # Scanner information. Reading json file for this
                try:
                    mri_metadata = read_s3_contents(cos_client, app.config['BUCKET'],
                                                        json_filename)
                    metadata = json.loads(mri_metadata.decode("utf-8"))
                    try:
                        serial_number = metadata["DeviceSerialNumber"]
                    except:
                        # Hardcoding the serial number since NKIRS appears to have only 1 scanner
                        serial_number = 35390
                    manufacturer = metadata["Manufacturer"]
                    manufacturerModel = metadata["ManufacturersModelName"]
                    existing_scanner = Scanner.query.filter(Scanner.brand == metadata["Manufacturer"],
                                                            Scanner.model == metadata["ManufacturersModelName"],
                                                            Scanner.source_id == serial_number,
                                                            Scanner.source_dataset_id == dataset_object.id).first()
                    scanner_teslas = None
                    if metadata['MagneticFieldStrength'] in ['3', 3]:
                        scanner_teslas = 'three'
                    else:
                        print(f"There is a scanner with different teslas: {metadata['MagneticFieldStrength']}")
                    if existing_scanner:
                        existing_scanner.teslas = scanner_teslas
                        db.session.merge(existing_scanner)
                    else:
                        existing_scanner = Scanner(brand = metadata['Manufacturer'],
                                                   model = metadata['ManufacturersModelName'],
                                                   source_id = serial_number,
                                                   source_dataset_id=dataset_object.id, teslas = scanner_teslas)
                        db.session.add(existing_scanner)
                    db.session.commit()
                except cos_client.meta.client.exceptions.NoSuchKey:
                    print(f"Can't find image metadata for file {i.key}")
                    continue
                # Three types of interest for us in NKIRS. Ignoring T2w and FLAIR
                if ("T1w" in i.key):
                    image_type = 'T1'
                elif "task-rest" in i.key:
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
                elif 'T2w' in i.key or 'FLAIR' in i.key:
                    pass
                else:
                    print(f"ERROR: Unable to select image type: {i.key}")
                    continue
                # TODO: Need approval to access condition for NKIRS dataset. Setting HC as condition ATM
                condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
                try:
                    # Extract subject external id from the name of the file
                    subject_external_id = re.search(re.compile('sub-A\d{8}'), i.key).group(0)
                    # Ignoring files that are missing age or gender
                    if subject_external_id in avoid:
                        print(f'Excluding file since subject has no age or no gender: {i.key}')
                        continue
                except:
                    print(f"ERROR: Unable to extract external id from file: {i.key}")
                try:
                    # Extract visit external id from the name of the file
                    visit_external_id = re.search(re.compile('/ses-.{4}'), i.key).group(0).replace("ses-", "").replace("/", "")
                except:
                    print(f"ERROR: Unable to extract visit external id from file {i.key}")

                subject_info_df = open_subject_metadata_file(app, cos_client, subject_external_id)                
                
                visit_days_since_baseline = calculate_days_since_baseline(subject_info_df, visit_external_id)
                existing_subject = Subject.query.filter(Subject.external_id == subject_external_id,
                                                        Subject.source_dataset_id == dataset_object.id).first()
                if existing_subject:
                    existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                        Visit.subject_id == existing_subject.id,
                                                        Visit.source_dataset_id == dataset_object.id).first()

                    if existing_visit:
                        existing_visit.condition_id = condition.id
                        existing_visit.days_since_baseline = visit_days_since_baseline
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