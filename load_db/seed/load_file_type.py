import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import FileType
from config.globals import ENVIRONMENT
import pdb



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        file_list = ['T1w_2_MNI_rigid_w_scale_image', 'T1w_2_MNI_rigid_w_scale_affine', 'brain_mask_T1w_native_space']
        for file_designation in file_list:
            existing_file_type = FileType.query.filter(FileType.designation == file_designation).first()
            if existing_file_type:
                existing_file_type.designation = file_designation
                db.session.merge(existing_file_type)
            else:
                file_type_db = FileType(designation=file_designation)
                db.session.add(file_type_db)
            db.session.commit()
