from botocore.client import Config
import ibm_boto3
import os
import sys
from pathlib import Path
# from shutil import copyfile
# from tqdm import tqdm
# import tensorflow as tf
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app  # , db
from app.models import Image, FileType, PreprocessTask, PreprocessTaskFile
from config.globals import ENVIRONMENT
import pandas as pd
import os
import json

if __name__ == '__main__':
    # tf.get_logger().setLevel('ERROR')
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        cos_client = ibm_boto3.client('s3',
                                      ibm_api_key_id=app.config['COS_CREDENTIALS']['apikey'],
                                      ibm_service_instance_id=app.config['COS_CREDENTIALS']['resource_instance_id'],
                                      ibm_auth_endpoint=app.config['AUTH_ENDPOINT'],
                                      config=Config(signature_version='oauth'),
                                      endpoint_url=app.config['SERVICE_ENDPOINT'])
        image_file_type = FileType.query.filter(FileType.designation == 'T1w_2_MNI_rigid_w_scale_image').first()
        preprocess_task_file_query = PreprocessTaskFile.query.filter(PreprocessTaskFile.file_type_id==image_file_type.id)
        temp_dir = Path(__file__).parent / '../temp/'
        preprocess_files_df = pd.read_sql(preprocess_task_file_query.statement, preprocess_task_file_query.session.bind)

#         max_i = 5
        # df = preprocess_files_df.iloc[:max_i]
        # df['r'] = [df['preprocess_check_json'][i]['r'] for i in df.index]
        # df['proc_fn'] = df['preprocess_file_path'].copy()
        # df['raw_fn'] = df['preprocess_file_path'].copy()

        # for i, row in df.iterrows():  # preprocess_files_df.iterrows():
        #     print(i)
        #     im_id = row['image_id']

        #     proc_fn = temp_dir / 'proc' /\
        #         row['preprocess_file_path'].split('/')[-1]

        #     raw_im_query = Image.query.filter(Image.id == im_id).first()
        #     raw_fn = temp_dir / 'raw' / raw_im_query.image_path.split('/')[-1]

        #     df.at[i, 'proc_fn'] = proc_fn
        #     df.at[i, 'raw_fn'] = raw_fn
        #     # import ipdb; ipdb.set_trace()
        #     if ~proc_fn.exists():
        #         raw_fn.parent.mkdir(exist_ok=True)
        #         proc_fn.parent.mkdir(exist_ok=True)
        #         with open(proc_fn, 'wb') as f:
        #             cos_client.download_fileobj(app.config['BUCKET'],
        #                                         row.preprocess_file_path, f)
        #         with open(raw_fn, 'wb') as f:
        #             cos_client.download_fileobj(app.config['BUCKET'],
        #                                         raw_im_query.image_path, f)

        # used_cols = ['id', 'image_id', 'r', 'proc_fn', 'raw_fn']
        # df = df[used_cols]
        # df.to_csv(temp_dir / 'test_images.csv')
