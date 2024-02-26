from botocore.client import Config
import ibm_boto3
import os
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from load_db.ADNI import get_visit_code
from config.globals import ENVIRONMENT
from app import create_app, db
from app.models import Image, SourceDataset, Subject, Visit, Scanner
from load_db.ADNI import get_adni_dx
from load_file.utils import read_s3_contents
from datetime import timedelta
from lxml import etree
import numpy as np
pd.options.mode.chained_assignment = None

if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        adni_meta_merge = pd.read_csv('./data/ADNI_all_images/metadata/ADNI_tabular/ADNIMERGE.csv')
        adni_meta_merge.set_index('IMAGEUID', inplace=True)
        adni_meta_merge['EXAMDATE'] = pd.to_datetime(adni_meta_merge['EXAMDATE'], infer_datetime_format=True) # format='%Y-%m-%d')
        adni_meta_merge = adni_meta_merge.sort_values(by='EXAMDATE', ascending=True)
        adni_vitals = pd.read_csv('./data/ADNI_all_images/metadata/ADNI_tabular/VITALS.csv')
        adni_vitals.set_index('RID', inplace=True)
        adni_vitals['USERDATE'] = pd.to_datetime(adni_vitals['USERDATE'],  infer_datetime_format=True) #format='%Y-%m-%d')
        adni_vitals = adni_vitals.sort_values(by='USERDATE', ascending=True)
        adni_merge_t1 = pd.read_csv('./data/ADNI_all_images/metadata/T1_merge_nodup.csv')
        adni_merge_t1.set_index('Subject ID', inplace=True)
        adni_merge_t1['Study Date'] = pd.to_datetime(adni_merge_t1['Study Date'], infer_datetime_format=True) #format='%m/%d/%Y')
        adni_merge_t1 = adni_merge_t1.sort_values(by='Study Date', ascending=True)

        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'ADNI').first()
        cos_client = ibm_boto3.resource('s3',
                                     ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                     ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                     ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                     config=Config(signature_version='oauth'),
                                     endpoint_url=app.config['SERVICE_ENDPOINT'])
        bucket = cos_client.Bucket(app.config['BUCKET'])
        prefix = 'ADNI_all_images'
        count = 0
        no_data = {
            'miss_image_id_T1_merge': set(),
            'miss_subj_T1_merge': set(),
            'no_baseline_visit': set(),
            'miss_image_on_ADNIMERGE': set(),
            'unable_open_meta_file': set(),
            'no_bmi': set(),
            'no_condition': set(),
            'negative_days_since_bl': set(),
            'exact_examdate': set(),
        }
        distinct_images = set()
        for i in bucket.objects.filter(Prefix=prefix):
            if i.key.endswith('nii.gz') or i.key.endswith('nii'):
                count+=1
                # Remove continuous appearances of _ from the key
                prev = ''
                new_str = ''
                for curr in i.key:
                    if prev == '_' and curr == '_':
                        continue
                    new_str+=curr
                    prev = curr

                subject_data = new_str.split('/')[-1].split('ADNI_')[1].split('_')
                subject_external_id = int(subject_data[2])
                subject_PTID = '_'.join(subject_data[0:3])
                try: # If the subject is not on metadata, skip this image
                    adni_merge_subject = adni_merge_t1.loc[subject_PTID]
                except:
                    no_data['miss_subj_T1_merge'].add(subject_PTID)
                    continue
                if not adni_merge_subject.empty:
                    image_id = int(new_str.split('.nii')[0].split('_I')[-1])
                    distinct_images.add(image_id)
                    adni_baseline = adni_merge_subject.copy()
                    # Pandas can return a Series or a DataFrame. Verify always which one is returned
                    # Get the oldest baseline visit of the subject. Subj can have BL from several ADNI phases.
                    if isinstance(adni_baseline, pd.Series): 
                        adni_baseline = adni_baseline if get_visit_code(adni_baseline) == 'bl' else None
                    else:
                        adni_baseline = adni_baseline[adni_baseline.apply(lambda x: get_visit_code(x)=='bl',axis=1)]
                    if adni_baseline is None or adni_baseline.empty:
                        no_data['no_baseline_visit'].add(subject_external_id)
                        continue
                    # In case adni_baseline has more than 1 baseline and is a DataFrame instead of a Series
                    adni_baseline = adni_baseline.iloc[0] if isinstance(adni_baseline.iloc[0], pd.Series) else adni_baseline
                    # Get the current image row from the metadata file
                    try:
                        if isinstance(adni_merge_subject, pd.Series):
                            current_image_df = adni_merge_subject if adni_merge_subject['Image ID'] == image_id else None
                            if current_image_df is None:
                                raise Exception("Not possible to find the image on Metadata")
                        else:
                            current_image_df = adni_merge_subject[adni_merge_subject['Image ID'] == image_id].iloc[0]
                    except:
                        no_data['miss_image_id_T1_merge'].add(image_id)
                        continue
                    
                    # Visit external id is composed of ADNI phase (1, 2, 3 or GO) and the visit code
                    visit_external_id = f"{current_image_df['Phase']}/{get_visit_code(current_image_df)}"
                    # Get the days from the study date of the baseline and the current image
                    days_since_baseline = (current_image_df['Study Date']-adni_baseline['Study Date']).days
                    if days_since_baseline < 0:
                        no_data['negative_days_since_bl'].add(image_id)
                    # Symptoms from the metadata file
                    symptoms = dict()
                    if pd.notnull(current_image_df['MMSE Total Score']):
                        symptoms['mmse'] = current_image_df['MMSE Total Score'] 
                    if pd.notnull(current_image_df['Global CDR']):
                        symptoms['cdr-global'] = current_image_df['Global CDR']
                    if pd.notnull(current_image_df['GDSCALE Total Score']):
                        symptoms['gdscale'] = current_image_df['GDSCALE Total Score']
                    if pd.notnull(current_image_df['NPI-Q Total Score']):
                        symptoms['npi-q'] = current_image_df['NPI-Q Total Score']
                    # At this moment, all ADNI files that we have are T1's. 
                    # We can check on weighting or in the name of the file in the future
                    type = 'T1'

                    # There are several ways to re-create the metadata file
                    split_key= '_br'
                    if not '_br' in new_str:
                        split_key = '_raw'
                    image_metadata = new_str.split('/')[-1].replace('_MR','').replace('nii.gz','xml').replace('nii','xml').split(split_key)
                    image_metadata = image_metadata[0]+'_'+image_metadata[1].split('_')[-2]+'_'+image_metadata[1].split('_')[-1]
                    image_metadata_prefix = 'ADNI_all_images/metadata/'
                    # First try, removing all the consecutive __ from the metadata
                    try: 
                        image_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'],image_metadata_prefix+image_metadata)
                    except cos_client.meta.client.exceptions.NoSuchKey:
                        image_metadata = i.key.split('/')[-1].replace('_MR','').replace('nii.gz','xml').replace('nii','xml').split(split_key)
                        image_metadata_first_part = image_metadata[0]
                        if image_metadata_first_part[-1] == '_':
                            image_metadata_first_part = image_metadata_first_part[:-1]
                        image_metadata = image_metadata_first_part+'_'+image_metadata[1].split('_')[-2]+'_'+image_metadata[1].split('_')[-1]
                        # Second try, keeping all the consecutive __ from the metadata but removing one _
                        try: 
                            image_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'],image_metadata_prefix+image_metadata)
                        except cos_client.meta.client.exceptions.NoSuchKey:
                            # Last try, same as before but keeping the _ in the middle of two parts
                            try: 
                                image_metadata = i.key.split('/')[-1].replace('_MR','').replace('nii.gz','xml').replace('nii','xml').split(split_key)
                                image_metadata = image_metadata_first_part+'__'+image_metadata[1].split('_')[-2]+'_'+image_metadata[1].split('_')[-1]
                                image_metadata_content = read_s3_contents(cos_client, app.config['BUCKET'],image_metadata_prefix+image_metadata)
                            except:
                                no_data['unable_open_meta_file'].add(i.key)
                                # Unable to get the metadata file. Skipping the image file
                                continue

                    image_metadata_xml = etree.fromstring(image_metadata_content).find('.//imagingProtocol').find('.//protocolTerm')
                    # Getting the data of the Scanner from metadata file
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
                    existing_scanner = Scanner.query.filter(Scanner.brand == image_metadata_manufacturer,
                                                            Scanner.model == image_metadata_model,
                                                            Scanner.source_id == serial_number,
                                                            Scanner.source_dataset_id == dataset_object.id).first()


                    scanner_teslas = None
                    if image_metadata_field_strength == '3' or image_metadata_field_strength == 3:
                        scanner_teslas = 'three'
                    elif image_metadata_field_strength == '1.5' or image_metadata_field_strength == 1.5:
                        scanner_teslas = 'one_and_a_half'
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
                    # This sometimes fails because ADNIMERGE doesnt have that Image ID
                    try:
                        adni_merge_curr_image = adni_meta_merge.loc[image_id]
                    except:
                        no_data['miss_image_on_ADNIMERGE'].add(image_id)
                        # Recreating the ADNIMERGE data
                        adni_merge_curr_image = {
                            'DX': current_image_df['Research Group'],
                            'RID': subject_external_id,
                            'EXAMDATE': current_image_df['Study Date']
                        }
                    # Fetching ADNI_VITALS data from VITALS csv, to get the BMI
                    try:
                        vitals_row_rid = adni_vitals.loc[adni_merge_curr_image['RID']]
                    except:
                        vitals_row_rid = None
                    delta = timedelta(60)
                    if isinstance(vitals_row_rid,pd.Series):
                        if (vitals_row_rid['USERDATE'] < adni_merge_curr_image['EXAMDATE']+delta) and\
                            (vitals_row_rid['USERDATE'] > adni_merge_curr_image['EXAMDATE']-delta):
                            if vitals_row_rid['USERDATE'] == adni_merge_curr_image['EXAMDATE']:
                                no_data['exact_examdate'].add(image_id)
                            vitals_row = vitals_row_rid
                        else:
                            vitals_row = None
                    elif vitals_row_rid is not None:
                        vitals_row = vitals_row_rid[vitals_row_rid['USERDATE']==adni_merge_curr_image['EXAMDATE']]
                        if vitals_row.empty:
                            vitals_row = vitals_row_rid.loc[(vitals_row_rid['USERDATE'] < adni_merge_curr_image['EXAMDATE']+delta) &
                                (vitals_row_rid['USERDATE'] > adni_merge_curr_image['EXAMDATE']-delta)]
                        else:
                            no_data['exact_examdate'].add(image_id)
                        if not vitals_row.empty and isinstance(vitals_row, pd.DataFrame):
                            vitals_row = vitals_row.iloc[0]
                    
                    try:
                        if vitals_row['VSWEIGHT'] == -4 or vitals_row['VSHEIGHT'] == -4:
                            # Unable to get the BMI
                            no_data['no_bmi'].add(image_id)
                            bmi = None
                        else:
                            w_factor = 1
                            h_factor = 1
                            if vitals_row['VSWTUNIT'] == 1: # Weight is in pounds
                                w_factor = 0.45359237 
                            if vitals_row['VSHTUNIT'] == 1: # Height is in inches
                                h_factor = 2.54
                            # Transform weight to KG and Height to Meters, then apply BMI=W/(H^2)
                            bmi = (vitals_row['VSWEIGHT'] * w_factor) / (((vitals_row['VSHEIGHT'] * h_factor)/100)**2)
                            if not pd.notnull(bmi):
                                bmi = None
                    except:
                        # Unable to get the BMI
                        no_data['no_bmi'].add(image_id)
                        bmi = None
                    condition = get_adni_dx(adni_merge_curr_image['DX'])
                    existing_subject = Subject.query.filter(Subject.external_id == str(subject_external_id),
                                                        Subject.source_dataset_id == dataset_object.id).first()
                    if existing_subject:
                        existing_visit = Visit.query.filter(Visit.external_id == visit_external_id,
                                                            Visit.subject_id == existing_subject.id,
                                                                Visit.source_dataset_id == dataset_object.id).first()
                        if condition:
                            condition_id = condition.id
                        else:
                            no_data['no_condition'].add(image_id)
                            condition_id = None
                        if existing_visit:
                            existing_visit.days_since_baseline = days_since_baseline
                            existing_visit.symptoms = symptoms
                            existing_visit.condition_id = condition_id
                            existing_visit.bmi=bmi
                            db.session.merge(existing_visit)
                        else:
                            existing_visit = Visit(external_id=visit_external_id, subject_id=existing_subject.id,
                                                 source_dataset_id=dataset_object.id, days_since_baseline=days_since_baseline,
                                                   symptoms=symptoms, condition_id=condition_id, bmi=bmi)
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
                            existing_image.days_since_baseline = days_since_baseline
                            existing_image.metadata_json = image_metadata_dict
                            existing_image.scanner_id = existing_scanner.id
                            db.session.merge(existing_image)
                        else:
                            image_db = Image(visit_id=existing_visit.id, subject_id=existing_subject.id,
                                                 source_dataset_id=dataset_object.id,
                                                 image_path=i.key, file_size=i.size,
                                             type=type, metadata_json=image_metadata_dict,
                                             days_since_baseline=days_since_baseline,
                                             scanner_id=existing_scanner.id)
                            db.session.add(image_db)
                        db.session.commit()
                    else:
                        print(f'ERROR: missing subject id {subject_external_id}')
        print('\nBalance:')
        for i in no_data:
            print(f'    {i}:{len(no_data[i])}')
        print(f'Image count: {count} and distinct ids {len(distinct_images)}')