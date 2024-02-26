from pathlib import Path
from botocore.client import Config
import ibm_boto3
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from load_db.AIBL import extract_symptoms, get_aibl_dx
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner, Condition
from load_file.utils import read_s3_contents
from lxml import etree
pd.options.mode.chained_assignment = None

def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))

avg_days_per_month = 30.4167
if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        aibl_merge_df = pd.read_csv(Path(__file__).parent / '../../data/AIBL_metadata/aibl_merged_data.csv')
        aibl_merge_df['EXAMDATE'] = pd.to_datetime(aibl_merge_df['EXAMDATE'])
        aibl_merge_df.set_index('Image ID', inplace=True)
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'AIBL').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])

        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'AIBL'
        for i in bucket.objects.filter(Prefix=prefix):
            if i.key.startswith(prefix) and i.size != 0 and i.key.endswith('nii.gz') and ('MPRAGE' in i.key or 't1' in i.key):
                try:
                    subject_external_id = int(i.key.split('/')[-1].split('AIBL_')[1].split('_')[0])
                except:
                    print(f"Error: {i.key}")
                aibl_merge_subject = aibl_merge_df[aibl_merge_df['RID'] == subject_external_id]
                image_external_id = int(i.key.split('_')[-1].replace('.nii.gz', '').replace('I', ''))
                if not aibl_merge_subject.empty:
                    image_metadata = aibl_merge_df.loc[image_external_id]
                    image_date = image_metadata['EXAMDATE']
                    aibl_merge_subject_baseline = aibl_merge_subject[aibl_merge_subject['VISCODE'] == 'bl']
                    try: 
                        visit_days_since_baseline = (image_metadata['EXAMDATE']-aibl_merge_subject_baseline['EXAMDATE'].values[0]).days
                    except:
                        # The current subject is missing baseline visit, so we calculate the days using
                        # the avg days per month and the visit code (m18, m36, m54...)
                        visit_days_since_baseline = int(avg_days_per_month * int(image_metadata['VISCODE'].replace("m","")))
                    visit_external_id = image_metadata['VISCODE']
                    if visit_days_since_baseline < -60:
                        print('IMAGE acquired more than 60 days before baseline')
                        continue
                    type = None
                    # If type is none Raise exception, change in all datasets.
                    if 'MPRAGE' in i.key or 't1' in i.key:
                        type = 'T1'
                    else:
                        print('ERROR: Not a T1 image')
                        continue
                    try:
                        image_metadata = i.key.split('/')[-1].replace('_MR','').replace('nii.gz','xml').split('__')
                        image_metadata = image_metadata[0]+'_'+image_metadata[1].split('_')[-2]+'_'+image_metadata[1].split('_')[-1]
                    except cos_client.meta.client.exceptions.NoSuchKey:
                        print(f"Can't find image metadata for file {image_metadata}, trying with other format")
                    except IndexError:
                        try:
                            image_metadata = i.key.split('/')[-1].replace('_MR','').replace('nii.gz','xml').split('br_raw')
                            image_metadata = image_metadata[0]+image_metadata[1].split('_')[-2]+'_'+image_metadata[1].split('_')[-1]
                        except IndexError:
                            print(f'Error... {image_metadata} and key was {i.key}')
                            continue
                    image_metadata = 'AIBL/AIBL_metadata/'+image_metadata
                    try:
                        image_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'],image_metadata)
                    except Exception as e:
                        print(f'{image_metadata}, cos {cos_client} and error {e}')
                        continue
                    image_metadata_xml = etree.fromstring(image_metadata_content).find('.//imagingProtocol').find('.//protocolTerm')

                    serial_number = None
                    image_metadata_manufacturer = image_metadata_xml.xpath("//protocol[@term='Manufacturer']")[0].text
                    image_metadata_model = image_metadata_xml.xpath("//protocol[@term='Mfg Model']")[0].text
                    image_metadata_field_strength = image_metadata_xml.xpath("//protocol[@term='Field Strength']")[
                        0].text
                    image_metadata_dict = {'manufacturer':image_metadata_manufacturer,
                                           'model':image_metadata_model,
                                           'field_strength':image_metadata_field_strength,
                                           'acquisition_type':image_metadata_xml.xpath("//protocol[@term='Acquisition Type']")[0].text,
                                           'weighting':image_metadata_xml.xpath("//protocol[@term='Weighting']")[0].text,
                                           'pulse_sequence':image_metadata_xml.xpath("//protocol[@term='Pulse Sequence']")[0].text,
                                           'slice_thickness':image_metadata_xml.xpath("//protocol[@term='Slice Thickness']")[0].text,
                                           'TE':image_metadata_xml.xpath("//protocol[@term='TE']")[0].text,
                                           'TR':image_metadata_xml.xpath("//protocol[@term='TR']")[0].text,
                                           'TI':image_metadata_xml.xpath("//protocol[@term='TI']")[0].text,
                                           'coil':image_metadata_xml.xpath("//protocol[@term='Coil']")[0].text,
                                           'flip_angle':image_metadata_xml.xpath("//protocol[@term='Flip Angle']")[0].text,
                                           'acquisition_plane':image_metadata_xml.xpath("//protocol[@term='Acquisition Plane']")[0].text,
                                           'matrix_X':image_metadata_xml.xpath("//protocol[@term='Matrix X']")[0].text,
                                           'matrix_Y':image_metadata_xml.xpath("//protocol[@term='Matrix Y']")[0].text,
                                           'matrix_Z':image_metadata_xml.xpath("//protocol[@term='Matrix Z']")[0].text,
                                           'pixel_spacing_X':image_metadata_xml.xpath("//protocol[@term='Pixel Spacing X']")[0].text,
                                           'pixel_spacing_Y':image_metadata_xml.xpath("//protocol[@term='Pixel Spacing Y']")[0].text}
                    symptoms = extract_symptoms(aibl_merge_df.loc[image_external_id])
                    scanner_teslas = None
                    if int(float(image_metadata_field_strength)) == 3:
                        scanner_teslas = 'three'
                    elif float(image_metadata_field_strength) == 1.5:
                        scanner_teslas = 'one_and_a_half'
                    else:
                        print(f'Unable to get scanner strength: {image_metadata_field_strength}')
                        continue
                    existing_scanner = Scanner.query.filter(Scanner.brand == image_metadata_manufacturer,
                                                            Scanner.model == image_metadata_model,
                                                            Scanner.source_id == serial_number,
                                                            Scanner.source_dataset_id == dataset_object.id,
                                                            Scanner.teslas == scanner_teslas).first()


                    if existing_scanner:
                        existing_scanner.teslas = scanner_teslas
                        db.session.merge(existing_scanner)
                    else:
                        existing_scanner = Scanner(brand=image_metadata_manufacturer,
                                                   model=image_metadata_model,
                                                   source_id = serial_number,
                                                   source_dataset_id=dataset_object.id, teslas=scanner_teslas)
                        db.session.add(existing_scanner)
                    db.session.commit()

                    existing_subject = Subject.query.filter(Subject.external_id == str(subject_external_id),
                                                        Subject.source_dataset_id == dataset_object.id).first()
                    if existing_subject:
                        existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                            Visit.subject_id == existing_subject.id,
                                                                Visit.source_dataset_id == dataset_object.id).first()
                        visit_dx = aibl_merge_df.loc[image_external_id]['DXCURREN']
                        condition = get_aibl_dx(visit_dx, symptoms)
                        if condition:
                            condition_id = condition.id
                        else:
                            condition_id = None
                        if existing_visit:
                            existing_visit.days_since_baseline = visit_days_since_baseline
                            existing_visit.symptoms = symptoms
                            existing_visit.condition_id = condition_id
                            db.session.merge(existing_visit)
                        else:
                            existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                                 source_dataset_id=dataset_object.id, days_since_baseline=visit_days_since_baseline,
                                                   symptoms=symptoms, condition_id=condition_id)
                            db.session.add(existing_visit)
                        db.session.commit()
                        existing_image = Image.query.filter(Image.visit_id == existing_visit.id,
                                                            Image.subject_id == existing_subject.id,
                                                            Image.source_dataset_id == dataset_object.id,
                                                            Image.image_path == i.key).first()
                        if existing_image:
                            existing_image.image_path = i.key
                            existing_image.file_size = i.size
                            existing_image.type = type
                            existing_image.days_since_baseline = visit_days_since_baseline
                            existing_image.metadata_json = image_metadata_dict
                            existing_image.scanner_id = existing_scanner.id
                            db.session.merge(existing_image)
                        else:
                            image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                                 source_dataset_id=dataset_object.id,
                                                 image_path=i.key, file_size=i.size,
                                             type=type, metadata_json=image_metadata_dict,
                                             days_since_baseline=visit_days_since_baseline,
                                             scanner_id=existing_scanner.id)
                            db.session.add(image_db)
                        db.session.commit()
                    else:
                        print(f'ERROR: missing subject id {subject_external_id}')