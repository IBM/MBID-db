from botocore.client import Config
import ibm_boto3
# import types
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app
from utils import compare_dataset_bucket_disk
from config.globals import ENVIRONMENT


if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        cos_client = ibm_boto3.resource('s3',
                                        ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                        ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                        ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                        config=Config(signature_version='oauth'),
                                        endpoint_url=app.config['SERVICE_ENDPOINT'])
        bucket = cos_client.Bucket(app.config['BUCKET'])

        # oasis_dir = "/data/datasets/OASIS"
        # compare_dataset_bucket_disk(oasis_dir, bucket)

        # openpain_dir = "/data/matias/openpain.org"
        # compare_dataset_bucket_disk(openpain_dir, bucket)

        # hcp_dir = "/data/datasets/HCP1200"
        # compare_dataset_bucket_disk(hcp_dir, bucket)

        # ppmi_dir = "/data2/eduardo/datasets/PPMI/nifti"
        # compare_dataset_bucket_disk(ppmi_dir, bucket, multidir_prefix=True)

        # sald_dir = "/data/datasets/SALD"
        # compare_dataset_bucket_disk(sald_dir, bucket, multidir_prefix=True)
        
        predict_dir = "/data1/chdi_disks/Disk3/PREDICT-HD/imaging_data"
        compare_dataset_bucket_disk(predict_dir, bucket, multidir_prefix=True)
        



