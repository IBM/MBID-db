from botocore.client import Config
import ibm_boto3
import types
import os
import sys
import pdb
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app
from utils import upload_directory
from config.globals import ENVIRONMENT




if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():


        cos_client = ibm_boto3.client('s3',
                                      ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                      ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                      ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                      config=Config(signature_version='oauth'),
                                      endpoint_url=app.config['SERVICE_ENDPOINT'])

        # ADNI dataset was moved using rclone because we were unable to download all the images (>800GB) into our server
        
        oasis_dir = "/data/datasets/OASIS"
        dataset_name = 'OASIS'
        #upload_directory(oasis_dir, dataset_name, cos_client, config.BUCKET)

        oasis_dir = "/data/matias/openpain.org"
        dataset_name = 'openpain'
        #upload_directory(oasis_dir, dataset_name, cos_client, config.BUCKET)

        hcp_dir = "/data/datasets/HCP1200"
        dataset_name = 'HCP1200'
        #upload_directory(hcp_dir, dataset_name, cos_client, app.config['BUCKET'])

        adni_dir = "/data/datasets/ADNI"
        dataset_name = 'ADNI'
        #upload_directory(adni_dir, dataset_name, cos_client, app.config['BUCKET'])

        ppmi_dir = "/data2/eduardo/datasets/PPMI/nifti"
        dataset_name = 'PPMI'
        # upload_directory(ppmi_dir, dataset_name, cos_client, app.config['BUCKET'])
        
        sald_dir = "/data/datasets/SALD"
        dataset_name = 'SALD'
        # upload_directory(sald_dir, dataset_name, cos_client, app.config['BUCKET'])

        aibl_dir = "/data/datasets/AIBL/images"
        dataset_name = 'AIBL'
        upload_directory(aibl_dir, dataset_name, cos_client, app.config['BUCKET'])

        predict_dir = "/data1/chdi_disks/Disk3/PREDICT-HD/imaging_data"
        dataset_name = 'PREDICT-HD'
        # upload_directory(predict_dir, dataset_name, cos_client, app.config['BUCKET'])

        can_cam_dir = "/data/datasets/Cam-CAN"
        dataset_name = 'Cam-CAN'
        # upload_directory(can_cam_dir, dataset_name, cos_client, app.config['BUCKET'])
