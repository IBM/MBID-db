import os
from botocore.client import Config
import ibm_boto3
import sys
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             '..')))
from app import create_app, db
from app.models import Image, SourceDataset
from config.globals import ENVIRONMENT


# Input variables
study = 'OpenPain'

# API setup
config = os.environ
app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))

with app.app_context():
    dataset_object =\
        SourceDataset.query.filter(SourceDataset.designation
                                   == study).first()    

    # Query those images that were assigned a 'failed' status
    fail_images = Image.query.filter(Image.type == 'T1',
                                     Image.preprocessed == 'failed',
                                     Image.source_dataset_id ==
                                        dataset_object.id).all()

    # Correct this issue
    for f_img in tqdm(fail_images):
        if f_img:
            f_img.preprocessed = 'minimal1'
            f_img.blocking_processing_id = None
            db.session.merge(f_img)
    db.session.commit()

    # Query those images that were locked
    fail_images = Image.query.filter(Image.type == 'T1',
                                     Image.preprocessed == 'locked',
                                     Image.source_dataset_id ==
                                        dataset_object.id).all()

    # Correct that issue too
    for f_img in tqdm(fail_images):
        if f_img:
            f_img.preprocessed = 'minimal1'
            f_img.blocking_processing_id = None
            db.session.merge(f_img)
    db.session.commit()
