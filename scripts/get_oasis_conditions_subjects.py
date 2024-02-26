import pandas as pd
import os
import sys
import pdb
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, Visit, Condition, SourceDataset, Image
from config.globals import ENVIRONMENT




if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        dataset_object = SourceDataset.query.filter(SourceDataset.designation == 'OASIS').first()
        images = db.session.query(Image,Condition).join(Visit,Image.visit_id==Visit.id).join(Condition).filter(Visit.source_dataset_id==dataset_object.id).all()
        image_conditions = []
        for image in images:
            image_dict = {'image_id':image[0].id,'subject_id':image[0].subject_id,'dx':image[1].designation}
            image_conditions.append(image_dict)

        image_df = pd.DataFrame.from_records(image_conditions)
        # image id, subject id, diagnostio
        image_df.to_csv('oasis_image_conditions.csv',index=False)