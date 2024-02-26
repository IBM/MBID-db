import pandas as pd
import os
import sys
import pdb
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, Visit, SourceDataset, Image, PreprocessTaskFile, FileType
from config.globals import ENVIRONMENT

if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'ADNI').first()
        image_file_type = FileType.query.filter(FileType.designation=='T1w_2_MNI_rigid_w_scale_image').first()
        images = db.session.query(Image, Subject,PreprocessTaskFile).join(Subject).join(PreprocessTaskFile).filter(
            PreprocessTaskFile.file_type_id == image_file_type.id).all()
        image_dayssincebaseline = []
        for image in images:
            image_dict = {'image_id': image[0].id, 'days_since_baseline': image[0].days_since_baseline,
                          'age_at_baseline': image[1].age_at_baseline,
                          'preprocess_file_path':image[2].preprocess_file_path,
                          'subject_id':image[0].subject_id}
            image_dayssincebaseline.append(image_dict)

        image_df = pd.DataFrame.from_records(image_dayssincebaseline)
        image_df.to_csv('adni_subjects_images_dayssincebaseline.csv', index=False)