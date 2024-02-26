import pandas as pd
import os
import sys
import pdb
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Image
from config.globals import ENVIRONMENT






''' script to restore deleted information from field `preprocessed` in table image, in this script we retrieve t1
preproccesed images from the table preprocess_task_file and mark those as preprocessed = t1minimal. In order to 
ensure that the data we are using is right, we obtain it manually with a SQL query against the db and manually reviewed
it before adding it to this script'''
if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        preprocess_task_file_restore_df = pd.read_csv('../data/scripts/preprocess_task_file_restore_image_preprocessed.csv')
        preprocess_task_file_restore_list_imageid = preprocess_task_file_restore_df['image_id'].to_list()
        all_images_db = Image.query.all()
        for image in all_images_db:
            if image.id in preprocess_task_file_restore_list_imageid:
                image.preprocessed = 'minimal1'
        db.session.commit()